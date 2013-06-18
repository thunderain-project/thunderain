package thunderainproject.thunderain.example.weblog.output

import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.mutable

import shark.SharkEnv
import shark.memstore2.column.ColumnBuilder
import shark.memstore2.{TablePartition, TablePartitionStats}

import spark.RDD
import spark.SparkContext._
import spark.streaming.DStream
import spark.storage.StorageLevel

import thunderainproject.thunderain.framework.output.{AbstractEventOutput, PrimitiveObjInspectorFactory, 
  WritableObjectConvertFactory}

import tachyon.client.{TachyonClient, OpType}

abstract class TachyonRDDOutput extends AbstractEventOutput {
  val tachyonURL = System.getenv("TACHYON_MASTER")
  val tachyonWarehousePath = System.getenv("TACHYON_WAREHOUSE_PATH")
  
  @transient val tachyonClient = TachyonClient.getClient(tachyonURL)
  @transient lazy val tachyonClientOnSlave = TachyonClient.getClient(tachyonURL)
  lazy val tablePath = tachyonWarehousePath + "/" + outputName
  @transient lazy val table = tachyonClientOnSlave.getRawTable(tablePath)
  
  if (!tachyonClient.exist(tachyonWarehousePath)) {
    tachyonClient.mkdir(tachyonWarehousePath)
  }
  
  val cleanBefore = if (System.getenv("DATA_CLEAN_TTL") == null) {
    -1
  } else {
    System.getenv("DATA_CLEAN_TTL").toInt
  }
  
  val COLUMN_SIZE = 1000
  private var checkpointTm = System.currentTimeMillis() / 1000
  val CHECKPOINT_INTERVAL = 600
  
  private def createMetaCol(rowNum: Long, part: Int) {
    val metaCol = table.getRawColumn(0)
    var file = metaCol.getPartition(part)
    if (file != null) {
      tachyonClientOnSlave.delete(file.getPath())
    }
    metaCol.createPartition(part)
    file = metaCol.getPartition(part)
    
    val os = metaCol.getPartition(part).getOutStream(OpType.WRITE_CACHE)
    val buf = ByteBuffer.allocate(8)
    buf.order(ByteOrder.nativeOrder()).putLong(rowNum).flip()
    os.write(buf)
    os.close()
  }
  
  override def preprocessOutput(stream: DStream[_]): DStream[_] = {
    if (tachyonClient.exist(tablePath)) {
      tachyonClient.delete(tablePath)
    }
    tachyonClient.createRawTable(tablePath, formats.length + 1)
    
    stream.transform((r, t) => r.map(c => (t.milliseconds / 1000, c)))
  }
  
  override def output(stream: DStream[_]) {
    stream.foreach(r => {      
      val statAccum = SharkEnv.sc.accumulableCollection(mutable.ArrayBuffer[(Int, TablePartitionStats)]())
      
      val tblRdd = if (cleanBefore == -1) {
        buildTachyonRdd(r, statAccum)
      } else {
        val rdd = SharkEnv.memoryMetadataManager.get(outputName) match {
          case None => buildTachyonRdd(r, statAccum)
          case Some(s) => zipTachyonRdd(s, r, statAccum)
        }
        val currTm = System.currentTimeMillis() / 1000
        if (currTm - checkpointTm >= CHECKPOINT_INTERVAL) {
          rdd.checkpoint()
          checkpointTm = currTm
        }
        rdd
      }
      
      tblRdd.persist(StorageLevel.MEMORY_ONLY)
      tblRdd.foreach(_ => Unit)
         
      // put rdd and statAccum to cache manager
      SharkEnv.memoryMetadataManager.put(outputName, tblRdd)
      SharkEnv.memoryMetadataManager.putStats(outputName, statAccum.value.toMap)
    })
  }
  
  private def buildTachyonRdd(rdd: RDD[_], 
     stat: spark.Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)]) 
  = {
    val newRdd = rdd.mapPartitionsWithIndex((index, iter) => {
      val objInspectors = formats.map(PrimitiveObjInspectorFactory.newPrimitiveObjInspector(_))
      val colBuilders = objInspectors.map(i => ColumnBuilder.create(i))
      colBuilders.foreach(c => c.initialize(COLUMN_SIZE))
      
      var numRows = 0;  
      iter.foreach(row => {
        val (t1, ((t2, t3), t4)) = row.asInstanceOf[(Object, ((Object, Object), Object))]
        Array(t1, t2, t3, t4).zipWithIndex.foreach(r => 
          colBuilders(r._2).append(r._1, objInspectors(r._2)))
        numRows += 1
      })
      
      val tblPartStats = new TablePartitionStats(colBuilders.map(_.stats), numRows)
      stat += (index, tblPartStats)
      
      colBuilders.zipWithIndex.foreach(c => {
        val col = table.getRawColumn(c._2 + 1)
        var file = col.getPartition(index)
        if (file != null) {
          tachyonClientOnSlave.delete(file.getPath())
        }
        col.createPartition(index)
        file = col.getPartition(index)
        
        val os = file.getOutStream(OpType.WRITE_CACHE)
        os.write(c._1.build)
        os.close()
      })
      createMetaCol(numRows, index)
      
      Iterator(new TablePartition(numRows, colBuilders.map(_.build)))
    })
    
    newRdd
  }
  
  private def zipTachyonRdd(oldRdd: RDD[_], newRdd: RDD[_],
    stat: spark.Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)]) = { 
    val zippedRdd = oldRdd.asInstanceOf[RDD[TablePartition]].zipPartitions(
      (i1: Iterator[TablePartition], i2: Iterator[Any]) => {
        val objInspectors = formats.map(PrimitiveObjInspectorFactory.newPrimitiveObjInspector(_))
        val colBuilders = objInspectors.map(i => ColumnBuilder.create(i))
        colBuilders.foreach(c => c.initialize(COLUMN_SIZE))
        
        var numRows = 0
        val currTm = System.currentTimeMillis() / 1000
        i1.foreach(t => {
          t.iterator.foreach(c => {
            val row = c.getFieldsAsList
            val tm = WritableObjectConvertFactory.writableObjectConvert(row.get(0), formats(0)).asInstanceOf[Long]
            
            if (row.get(1) != null && row.get(2) != null && (currTm - tm) < cleanBefore) {
              colBuilders(0).append(tm: java.lang.Long, objInspectors(0))
              colBuilders(1).append(
                WritableObjectConvertFactory.writableObjectConvert(row.get(1), formats(1)), objInspectors(1))
              colBuilders(2).append(
                WritableObjectConvertFactory.writableObjectConvert(row.get(2), formats(2)), objInspectors(2))
              colBuilders(3).append(
                 WritableObjectConvertFactory.writableObjectConvert(row.get(3), formats(3)), objInspectors(3))

              numRows += 1
            }
          })      
        })
        
        i2.foreach(row => {
          val (t1, ((t2, t3), t4)) = row.asInstanceOf[(Object, ((Object, Object), Object))]
          Array(t1, t2, t3, t4).zipWithIndex.foreach(r => 
          colBuilders(r._2).append(r._1.asInstanceOf[Object], objInspectors(r._2)))
          numRows += 1
        })    

        val tblPartStats = new TablePartitionStats(colBuilders.map(_.stats), numRows)
      
        Iterator(new TablePartition(numRows, colBuilders.map(_.build)))
      }, newRdd.asInstanceOf[RDD[Any]]).mapPartitionsWithIndex((index, iter) => {
        val partition = iter.next()
        partition.toTachyon.zipWithIndex.foreach(r => {
          val col = table.getRawColumn(r._2)
          var file = col.getPartition(index)
          if (file != null) {
            tachyonClientOnSlave.delete(file.getPath())
          }
          col.createPartition(index)
          file = col.getPartition(index)
          val os = file.getOutStream(OpType.WRITE_CACHE)
          os.write(r._1)
          os.close()

        })
        Iterator(partition)
      })
        
    zippedRdd
  }
}
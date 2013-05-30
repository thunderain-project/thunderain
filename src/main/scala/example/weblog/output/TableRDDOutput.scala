package example.weblog.output

import scala.collection.mutable

import shark.SharkEnv
import shark.memstore2.column.ColumnBuilder
import shark.memstore2.TablePartition
import shark.memstore2.TablePartitionStats

import spark.RDD
import spark.SparkContext._
import spark.streaming.DStream
import spark.storage.StorageLevel

import stream.framework.output.AbstractEventOutput
import stream.framework.output.PrimitiveObjInspectorFactory
import stream.framework.output.WritableObjectConvertFactory

abstract class TableRDDOutput extends AbstractEventOutput {
  val cleanBefore = if (System.getenv("DATA_CLEAN_TTL") == null) {
    -1
  } else {
    System.getenv("DATA_CLEAN_TTL").toInt
  }
  
  val COLUMN_SIZE = 1000
  private var checkpointTm = System.currentTimeMillis() / 1000
  val CHECKPOINT_INTERVAL = 600
  
  override def preprocessOutput(stream: DStream[_]): DStream[_] = {
    stream.transform((r, t) => r.map(c => (t.milliseconds / 1000, c)))
  }
  
  override def output(stream: DStream[_]) {
    stream.foreach(r => {      
      val statAccum = SharkEnv.sc.accumulableCollection(mutable.ArrayBuffer[(Int, TablePartitionStats)]())
      
      val tblRdd = if (cleanBefore == -1) {
        buildTableRdd(r, statAccum)
      } else {
        val rdd = SharkEnv.memoryMetadataManager.get(outputName) match {
          case None => buildTableRdd(r, statAccum)
          case Some(s) => zipTableRdd(s, r, statAccum)
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
  
  private def buildTableRdd(rdd: RDD[_], 
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
      
      Iterator(new TablePartition(numRows, colBuilders.map(_.build)))
    })
    
    newRdd
  }
  
  private def zipTableRdd(oldRdd: RDD[_], newRdd: RDD[_],
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
      }, newRdd.asInstanceOf[RDD[Any]])
        
    zippedRdd
  }
}
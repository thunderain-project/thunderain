package example.weblog.output

import scala.collection.mutable

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io._

import shark.SharkEnv
import shark.memstore2.column.ColumnBuilder
import shark.memstore2.TablePartition
import shark.memstore2.TablePartitionStats

import spark.RDD
import spark.SparkContext._
import spark.streaming.DStream
import spark.storage.StorageLevel

import stream.framework.output.AbstractEventOutput

class WebLogOutput extends AbstractEventOutput {
  private var outputName: String = _
  private var jobName: String = _
  val cleanBefore = if (System.getenv("DATA_CLEAN_TTL") == null) {
    -1
  } else {
    System.getenv("DATA_CLEAN_TTL").toInt
  }
  
  private var types: Array[String] = _
  val COLUMN_SIZE = 1000
  
  override def setOutputName(name: String) {
    jobName = name
    outputName = name + "_cached"
    
    jobName match {
      case "item_view" => types = Array("Long", "String", "Long", "Long")
      case "subcategory_view" => types = Array("Long", "String", "String", "Long")
      case _ => throw new Exception("unkown job name " + jobName)
    }
  }
  
  private def recursiveFetchRdd(rdd: RDD[_], q: mutable.ArrayBuffer[Int]) {
    for (d <- rdd.dependencies) {
      println(">>>>rdd " + d.rdd)
      q += 1
      recursiveFetchRdd(d.rdd, q)
    }
  }
  
  
  override def output(stream: DStream[_]) {
    stream.foreach(r => {      
      val statAccum = SharkEnv.sc.accumulableCollection(mutable.ArrayBuffer[(Int, TablePartitionStats)]())
      
      val tblRdd = if (cleanBefore == -1) {
        buildTableRdd(r, statAccum)
      } else {
        SharkEnv.memoryMetadataManager.get(outputName) match {
          case None => buildTableRdd(r, statAccum)
          case Some(s) => zipTableRdd(s, r, statAccum)
        }
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
      val objInspectors: Array[ObjectInspector] = types.map(
          PrimitiveObjInspectorFactory.newPrimitiveObjInspector(_))
      val colBuilders = objInspectors.map(i => ColumnBuilder.create(i))
      colBuilders.foreach(c => c.initialize(COLUMN_SIZE))
      
      var numRows = 0;
      val currTime = (System.currentTimeMillis() / 1000).asInstanceOf[Object];
      
      iter.foreach(row => {
        val (t1, ((t2, t3), t4)) = (currTime, row.asInstanceOf[((Object, Object), Object)])
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
        val objInspectors: Array[ObjectInspector] = types.map(
            PrimitiveObjInspectorFactory.newPrimitiveObjInspector(_))
        val colBuilders = objInspectors.map(i => ColumnBuilder.create(i))
        colBuilders.foreach(c => c.initialize(COLUMN_SIZE))
        
        var numRows = 0
        val currTm = System.currentTimeMillis() / 1000
        i1.foreach(t => {
          t.iterator.foreach(c => {
            val row = c.getFieldsAsList
            val tm = row.get(0).asInstanceOf[LongWritable].get
            
            if (row.get(1) != null && row.get(2) != null && (currTm - tm) < cleanBefore) {
              colBuilders(0).append(tm: java.lang.Long, objInspectors(0))
              colBuilders(1).append(
                row.get(1).asInstanceOf[Text].toString, objInspectors(1))
            
              jobName match {
        	    case "item_view" => 
        	      colBuilders(2).append(
        	        row.get(2).asInstanceOf[LongWritable].get: java.lang.Long, objInspectors(2))
        	    case "subcategory_view" => 
        	      colBuilders(2).append(
        	        row.get(2).asInstanceOf[Text].toString, objInspectors(2))
        	    case _ => throw new Exception("unkown job name " + jobName)
              }
              colBuilders(3).append(
                row.get(3).asInstanceOf[LongWritable].get: java.lang.Long, objInspectors(3))
              numRows += 1
            }
          })      
        })
        
        i2.foreach(row => {
          val (t1, ((t2, t3), t4)) = (currTm, row.asInstanceOf[((Object, Object), Object)])
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
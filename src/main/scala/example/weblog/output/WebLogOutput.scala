package example.weblog.output

import scala.collection.mutable

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io._

import shark.SharkEnv
import shark.memstore.{ColumnStats, ColumnNoStats}
import shark.memstore.TableStorage
import shark.memstore.TableStats

import spark.RDD
import spark.SparkContext._
import spark.streaming.DStream
import spark.rdd.UnionRDD
import spark.storage.StorageLevel

import stream.framework.output.AbstractEventOutput

class WebLogOutput extends AbstractEventOutput {
  private var outputName: String = _
  private var jobName: String = _
  val PART_NUM = Some(System.getenv("OUTPUT_PARTITION_NUM")).getOrElse("4").toInt
  val cleanBefore = Some(System.getenv("DATA_CLEAN_TTL")).getOrElse("600").toInt
  private var types: Array[String] = _
  
  override def setOutputName(name: String) {
    jobName = name
    outputName = name + "_cached"
    
    jobName match {
      case "item_view" => types = Array("Long", "String", "Long", "Long")
      case "subcategory_view" => types = Array("Long", "String", "String", "Long")
      case _ => throw new Exception("unkown job name " + jobName)
    }
  }
  
  override def output(stream: DStream[_]) {
    stream.foreach(r => {      
      val statAccum = SharkEnv.sc.accumulableCollection(mutable.ArrayBuffer[(Int, TableStats)]())
      
      val tblRdd = SharkEnv.cache.get(outputName) match {
        case None => buildTableRdd(r, statAccum)
        case Some(s) => zipTableRdd(s.asInstanceOf[RDD[TableStorage]], r, statAccum)
      }
      tblRdd.foreach(_ => Unit)
         
      // put rdd and statAccum to cache manager
      SharkEnv.cache.put(outputName, tblRdd, StorageLevel.MEMORY_ONLY)
      SharkEnv.cache.putStats(outputName, statAccum.value.toMap)
    })
  }
  
  private def buildTableRdd(rdd: RDD[_], 
    stat: spark.Accumulable[mutable.ArrayBuffer[(Int, TableStats)],(Int, TableStats)]) 
  = {
    val newRdd = rdd.mapPartitionsWithIndex((index, iter) => {
      val colBuilders = types.map(ColumnBuilderFactory.newColumnBuilder(_))
      val objInspectors: Array[ObjectInspector] = types.map(
          PrimitiveObjInspectorFactory.newPrimitiveObjInspector(_))
      
      var numRows = 0;
      val currTime = (System.currentTimeMillis() / 1000).asInstanceOf[Object];
      
      iter.foreach(row => {
        val (t1, ((t2, t3), t4)) = (currTime, row.asInstanceOf[((Object, Object), Object)])
        Array(t1, t2, t3, t4).zipWithIndex.foreach(r => 
          colBuilders(r._2).append(r._1, objInspectors(r._2)))
        numRows += 1
      })
      
      val columns = colBuilders.map(_.build)
      val tableStats = new TableStats(
        columns.map { c => c.stats match {
          case s: ColumnNoStats[_] => None
          case s: ColumnStats[_] => Some(s)
      }}, numRows)
      
      stat += (index, tableStats)      
      Iterator(new TableStorage(numRows, columns.map(_.format)))
    })
    
    newRdd
  }
  
  private def zipTableRdd(oldRdd: RDD[_], newRdd: RDD[_],
    stat: spark.Accumulable[mutable.ArrayBuffer[(Int, TableStats)],(Int, TableStats)]) = {
    val tblRdd = buildTableRdd(newRdd, stat)
    val zippedRdd = oldRdd.asInstanceOf[RDD[TableStorage]].zipPartitions(
      (i1: Iterator[TableStorage], i2: Iterator[TableStorage]) => {
        val colBuilders = types.map(ColumnBuilderFactory.newColumnBuilder(_))
        val objInspectors: Array[ObjectInspector] = types.map(
            PrimitiveObjInspectorFactory.newPrimitiveObjInspector(_))
        var numRows = 0
        val currTm = System.currentTimeMillis() / 1000
        
        Array(i1, i2).foreach(r => {
          r.foreach(t => { 
            t.iterator.foreach(c => {
        	    val row = c.getFieldsAsList()
        	    val tm = row.get(0).asInstanceOf[LongWritable].get
      
        	    if (row.get(1) != null && row.get(2) != null && (currTm - tm) < cleanBefore) {
                colBuilders(0).append(
                  row.get(0).asInstanceOf[LongWritable].get: java.lang.Long, objInspectors(0))
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
        })
        
        val columns = colBuilders.map(_.build)
        Iterator(new TableStorage(numRows, columns.map(_.format)))
      }, tblRdd)
        
    zippedRdd
  }
}
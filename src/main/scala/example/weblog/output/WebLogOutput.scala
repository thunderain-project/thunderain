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
      
      val newRdd = r.mapPartitionsWithIndex((index, iter) => {
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
        
        statAccum += (index, tableStats)
        
        Iterator(new TableStorage(numRows, columns.map(_.format)))
      })
      
      // union current RDD and previous RDD to a new RDD, put to cache
      val unionRdd = SharkEnv.cache.get(outputName) match {
        case None => newRdd
        case Some(r) => r.asInstanceOf[RDD[TableStorage]].union(newRdd)
      }

      //reduce the union RDD for better performance
      var i = 0l
      val reducedRdd = unionRdd.map(r => {i += 1; (i, r)}).reduceByKey((s1, s2) => {
        val colBuilders = types.map(ColumnBuilderFactory.newColumnBuilder(_))
        val objInspectors: Array[ObjectInspector] = types.map(
            PrimitiveObjInspectorFactory.newPrimitiveObjInspector(_))
        val storages = Array(s1, s2)
        var numRows = 0
        storages.foreach(r => {
          r.iterator.foreach(c => {
        	val row = c.getFieldsAsList()
          
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
          })
          numRows += r.size
        })
        
        val columns = colBuilders.map(_.build)
        
        new TableStorage(numRows, columns.map(_.format))
      }, PART_NUM).map(r => r._2)
      
      reducedRdd.foreach(_ => Unit)
      
      // put rdd and statAccum to cache manager
      SharkEnv.cache.put(outputName, reducedRdd, StorageLevel.MEMORY_ONLY)
      SharkEnv.cache.putStats(outputName, statAccum.value.toMap)

    })
  }
}
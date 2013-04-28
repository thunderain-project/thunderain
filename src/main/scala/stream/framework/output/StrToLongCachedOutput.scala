package stream.framework.output

import scala.collection.mutable

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import shark.SharkEnv
import shark.memstore.{ColumnStats, ColumnNoStats}
import shark.memstore.TableStorage
import shark.memstore.TableStats

import spark.RDD
import spark.streaming.DStream
import spark.rdd.UnionRDD
import spark.storage.StorageLevel

class StrToLongCachedOutput extends AbstractEventOutput {
  private var outputName: String = _
  
  override def setOutputName(name: String) {
    outputName = name + "_cached"
  }
  
  override def output(stream: DStream[_]) {
    stream.foreach(r => {      
      val statAccum = SharkEnv.sc.accumulableCollection(mutable.ArrayBuffer[(Int, TableStats)]())
      
      val newRdd = r.mapPartitionsWithIndex((index, iter) => {
        val types = Array("Long", "String", "Long")
        val colBuilders = types.map(ColumnBuilderFactory.newColumnBuilder(_))
        val objInspectors: Array[ObjectInspector] = types.map(
            PrimitiveObjInspectorFactory.newPrimitiveObjInspector(_))
        
        var numRows = 0;
        val currTime = System.currentTimeMillis() / 1000;
        
        iter.foreach(row => {
          val newRow = row.asInstanceOf[(String, Long)]
          
          colBuilders(0).append(currTime: java.lang.Long, objInspectors(0))
          colBuilders(1).append(newRow._1, objInspectors(1))
          colBuilders(2).append(newRow._2: java.lang.Long, objInspectors(2))
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
        case Some(r) => newRdd.union(r.asInstanceOf[RDD[TableStorage]])
      }
      
      // put rdd and statAccum to cache manager
      SharkEnv.cache.put(outputName, unionRdd, StorageLevel.MEMORY_ONLY)
      SharkEnv.cache.putStats(outputName, statAccum.value.toMap)
      
      unionRdd.foreach(_ => Unit)
    })
  }
  
}
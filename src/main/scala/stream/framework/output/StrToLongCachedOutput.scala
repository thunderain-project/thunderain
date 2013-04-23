package stream.framework.output

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

import shark.memstore.Column._
import shark.memstore.UncompressedColumnFormat._
import shark.memstore.ColumnStats._
import shark.SharkEnv
import shark.memstore.TableStorage

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
      val newRdd = r.mapPartitionsWithIndex((index, iter) => {
        val colBuilders = Array(
            new LongColumnBuilder(new LongColumnFormat(100), LongColumnNoStats), 
            new TextColumnBuilder(new TextColumnFormat(100), TextColumnNoStats),
            new LongColumnBuilder(new LongColumnFormat(100), LongColumnNoStats))
            
        val objInspectors: Array[ObjectInspector] = Array(
            PrimitiveObjectInspectorFactory.javaLongObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaLongObjectInspector)
            
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
        Iterator(new TableStorage(numRows, columns.map(_.format)))
      })
      
      val unionRdd = SharkEnv.cache.get(outputName) match {
        case None => newRdd
        case Some(r) => r.asInstanceOf[RDD[TableStorage]].union(newRdd)
      }
      
      SharkEnv.cache.put(outputName, unionRdd, StorageLevel.MEMORY_ONLY)
      unionRdd.foreach(_ => Unit)
    })
  }
  
}
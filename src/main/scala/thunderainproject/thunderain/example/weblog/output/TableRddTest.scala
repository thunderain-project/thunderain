package thunderainproject.thunderain.example.weblog.output

import scala.collection.mutable

import spark.RDD
import spark.storage.StorageLevel

import org.apache.hadoop.io._

import shark.SharkEnv
import shark.SharkContext
import shark.memstore2.TablePartition
import shark.memstore2.TablePartitionStats

object TableRddTest {

  def main(args: Array[String]) {
    System.setProperty("spark.cleaner.ttl", "600")
    SharkEnv.initWithSharkContext("Rdd Serde test")
    
    val itemCateRdd = SharkEnv.sc.asInstanceOf[SharkContext]
      .sql2rdd("CREATE TABLE item_test1_cached AS SELECT i_item_sk, i_category FROM item_ext")
    
    val itemCateRdd2 = SharkEnv.sc.asInstanceOf[SharkContext]
      .sql2rdd("CREATE TABLE item_test2_cached AS SELECT i_item_sk, i_category FROM item_ext")
    
    val types = Array("Long", "String")
    val COLUMN_SIZE = 1000
    
    while (true) {
      val startTm = System.currentTimeMillis()
      val statAccum = SharkEnv.sc.accumulableCollection(mutable.ArrayBuffer[(Int, TablePartitionStats)]())
      
      val rdd1 = SharkEnv.memoryMetadataManager.get("item_test1_cached") match {
        case Some(s) => s
        case None => throw new Exception("cannot get item_test1_cached rdd in cache");exit(1)
      }
      
      val rdd2 = SharkEnv.memoryMetadataManager.get("item_test2_cached") match {
        case Some(s) => s
        case None => throw new Exception("cannot get item_test2_cached rdd in cache");exit(1)
      }
          
      val tblPartRdd1 = rdd1.asInstanceOf[RDD[TablePartition]]
      val tblPartRdd2 = rdd2.asInstanceOf[RDD[TablePartition]]
      
      val newRdd = tblPartRdd1.zipPartitions(
        (i1: Iterator[TablePartition], i2: Iterator[TablePartition]) => {
        Iterator()
      }, tblPartRdd2)
      
      newRdd.persist(StorageLevel.MEMORY_ONLY)
      newRdd.foreach(_ => Unit)
//      SharkEnv.memoryMetadataManager.put("item_test_cached", newRdd)
//      SharkEnv.memoryMetadataManager.putStats("item_test_cached", statAccum.value.toMap)
      
      val stopTm = System.currentTimeMillis()
      
      println(">>>>> spend time " + (stopTm - startTm))
      Thread.sleep(5000)
    }
    
  }
}
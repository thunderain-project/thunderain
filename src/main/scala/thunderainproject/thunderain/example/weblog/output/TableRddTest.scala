/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package thunderainproject.thunderain.example.weblog.output

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.io._

import shark.SharkEnv
import shark.SharkContext
import shark.memstore2.{TablePartition, TablePartitionStats}
import org.apache.hadoop.hive.metastore.MetaStoreUtils

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

      val rdd1 = SharkEnv.memoryMetadataManager
        .getMemoryTable(MetaStoreUtils.DEFAULT_DATABASE_NAME,"item_test1_cached").map(_.tableRDD) match {
        case Some(s) => s
        case None => throw new Exception("cannot get item_test1_cached rdd in cache");exit(1)
      }

      val rdd2 = SharkEnv.memoryMetadataManager
        .getMemoryTable(MetaStoreUtils.DEFAULT_DATABASE_NAME,"item_test2_cached").map(_.tableRDD) match {
        case Some(s) => s
        case None => throw new Exception("cannot get item_test2_cached rdd in cache");exit(1)
      }

      val tblPartRdd1 = rdd1.asInstanceOf[RDD[TablePartition]]
      val tblPartRdd2 = rdd2.asInstanceOf[RDD[TablePartition]]

      val newRdd = tblPartRdd1.zipPartitions(tblPartRdd2){
        (i1: Iterator[TablePartition], i2: Iterator[TablePartition]) => {
          Iterator()
        }
      }

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

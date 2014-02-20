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
import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream

import org.apache.hadoop.hive.metastore.MetaStoreUtils
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory

import shark.SharkEnv
import shark.memstore2.column.ColumnBuilder
import shark.memstore2.{CacheType, TablePartition, TablePartitionStats}

import thunderainproject.thunderain.framework.output.{AbstractEventOutput,
  PrimitiveObjInspectorFactory, WritableObjectConvertFactory}

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
        val rdd = SharkEnv.memoryMetadataManager
          .getMemoryTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, outputName).map(_.getRDD.get) match {
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
      tblRdd.setName(outputName)
      val memoryTable = SharkEnv.memoryMetadataManager
        .getMemoryTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, outputName)
        .getOrElse(SharkEnv.memoryMetadataManager.createMemoryTable(
        MetaStoreUtils.DEFAULT_DATABASE_NAME, outputName, CacheType.MEMORY))
      memoryTable.put(tblRdd, statAccum.value.toMap)
    })
  }

  private def buildTableRdd(rdd: RDD[_],
     stat: org.apache.spark.Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)])
  = {
    val newRdd = rdd.mapPartitionsWithIndex((index, iter) => {
      val columnarStructOI = ObjectInspectorFactory.getColumnarStructObjectInspector(formats.toList.asJava,
          formats.map(PrimitiveObjInspectorFactory.newPrimitiveObjInspector(_)).toList.asJava)
      val colBuilders = columnarStructOI.getAllStructFieldRefs.asScala.map {
        s => ColumnBuilder.create(s) }.toArray
      val objInspectors = columnarStructOI.getAllStructFieldRefs.asScala.map {
        s => s.getFieldObjectInspector }.toArray
      colBuilders.foreach(c => c.initialize(COLUMN_SIZE))

      var numRows = 0;
      iter.foreach(row => {
        val (t1, ((t2, t3), t4)) = row.asInstanceOf[(Object, ((Object, Object), Object))]
        Array(t1, t2, t3, t4).zipWithIndex.foreach(r =>
          colBuilders(r._2).append(r._1, objInspectors(r._2)))
        numRows += 1
      })

      //val tblPartStats = new TablePartitionStats(colBuilders.map(_.stats), numRows)
      //stat += (index, tblPartStats)

      Iterator(new TablePartition(numRows, colBuilders.map(_.build)))
    })

    newRdd
  }

  private def zipTableRdd(oldRdd: RDD[_], newRdd: RDD[_],
    stat: org.apache.spark.Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)]) = {

    val zippedRdd = oldRdd.asInstanceOf[RDD[TablePartition]].zipPartitions(newRdd.asInstanceOf[RDD[Any]]){
      (i1: Iterator[TablePartition], i2: Iterator[Any]) => {
        val columnarStructOI = ObjectInspectorFactory.getColumnarStructObjectInspector(formats.toList.asJava,
            formats.map(PrimitiveObjInspectorFactory.newPrimitiveObjInspector(_)).toList.asJava)
        val colBuilders = columnarStructOI.getAllStructFieldRefs.asScala.map {
          s => ColumnBuilder.create(s) }.toArray
        val objInspectors = columnarStructOI.getAllStructFieldRefs.asScala.map {
          s => s.getFieldObjectInspector }.toArray
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

        //val tblPartStats = new TablePartitionStats(colBuilders.map(_.stats), numRows)

        Iterator(new TablePartition(numRows, colBuilders.map(_.build)))
      }
    }

    zippedRdd
  }
}

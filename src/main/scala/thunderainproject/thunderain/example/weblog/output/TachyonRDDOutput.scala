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

import org.apache.spark.Accumulable
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.Logging

import scala._
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent._
import ExecutionContext.Implicits.global

import shark.tachyon.TachyonTableWriter
import shark.execution.serialization.JavaSerializer
import shark.memstore2.ColumnarStructObjectInspector.IDStructField
import shark.{SharkContext, SharkEnvSlave, SharkEnv}
import shark.memstore2._

import thunderainproject.thunderain.framework.output.{AbstractEventOutput, PrimitiveObjInspectorFactory}

import tachyon.client.WriteType

import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorUtils, StructField, StructObjectInspector}

import java.nio.{ByteBuffer, ByteOrder}
import java.util.{List => JList, ArrayList => JArrayList}
import org.apache.hadoop.hive.metastore.MetaStoreUtils
import scala.Tuple2


abstract class TachyonRDDOutput extends AbstractEventOutput with Logging{
  var tachyonWriter: TachyonTableWriter = _
  var fieldNames: Array[String] = _
  var timeColumnIndex: Int = _

  val cleanBefore = if (System.getenv("DATA_CLEAN_TTL") == null) {
    -1
  } else {
    System.getenv("DATA_CLEAN_TTL").toInt
  }

  var cachedRDD: RDD[TablePartition] = _
  var tableKey: String = _

  val COLUMN_SIZE = 1000
  private var checkpointTm = System.currentTimeMillis() / 1000
  val CHECKPOINT_INTERVAL = 600

  def setOutputFormat(fieldNames: Array[String], fieldFormats: Array[String]) {
    this.fieldNames = fieldNames
    super.setOutputDataFormat(fieldFormats)
  }

  override def preprocessOutput(stream: DStream[_]): DStream[_] = {
    val sc = try{
      stream.context.sparkContext.asInstanceOf[SharkContext]  }
    catch {
      case _ => {
        logError("Failed to obtain a SharkContext instance")
        null
      }
    }
    if(sc != null){
      val resultSets = sc.sql("describe %s".format(outputName)).flatMap(_.split("\\t")).zipWithIndex
      setOutputFormat(resultSets.filter(_._2%3==0).map(_._1).toArray,
        resultSets.filter(_._2%3==1).map(_._1).toArray)

      timeColumnIndex = fieldNames.zipWithIndex.toMap.apply("time")
    }
    logDebug("fieldNames: " + fieldNames.mkString(",") + " formats: " + formats.mkString(","))
    //obtain the tachyonWriter from the shark util
    tableKey = MemoryMetadataManager.makeTableKey(MetaStoreUtils.DEFAULT_DATABASE_NAME, outputName)
    tachyonWriter = SharkEnv.tachyonUtil.createTableWriter(tableKey, None, formats.length + 1)

    stream.transform((r, t) => r.map(c => (t.milliseconds / 1000, c)))
  }

  override def output(stream: DStream[_]) {
    stream.foreach(r => {
      val statAccum =
        stream.context.sparkContext.accumulableCollection(mutable.ArrayBuffer[(Int, TablePartitionStats)]())

      val tblRdd = if (cleanBefore == -1) {
        buildTachyonRdd(r, statAccum)
      } else {
        cachedRDD =
          if(cachedRDD == null) buildTachyonRdd(r, statAccum)
          else zipTachyonRdd(cachedRDD, r, statAccum)
        val currTm = System.currentTimeMillis() / 1000
        if (cachedRDD != null && currTm - checkpointTm >= CHECKPOINT_INTERVAL) {
          cachedRDD.checkpoint()
          checkpointTm = currTm
        }
        cachedRDD
      }

      //dummy output stream
      tblRdd.foreach(_ => Unit) //to trigger spark job in order to evaluate it immediately

      // put rdd and statAccum to cache manager
      if (tachyonWriter != null && statAccum != null && SharkEnv.tachyonUtil.tableExists(tableKey, None)) {
        //persist the stats onto tachyon file system, otherwise Shark cannot read those data from tachyon later
        tachyonWriter.updateMetadata(ByteBuffer.wrap(JavaSerializer.serialize(statAccum.value.toMap)))
      }
    })
  }

  private def buildTachyonRdd(rdd: RDD[_],
                      stat: Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)]): RDD[TablePartition] = {
    logDebug("To build tachyon rdd")
    val inputRdd = prepareTablePartitionRDD(rdd, stat)
    inputRdd.cache()
    if(cachedRDD!=null)cachedRDD.unpersist()
    cachedRDD = inputRdd
    writeTablePartitionRDDToTachyon(inputRdd)
  }

  def zipTachyonRdd(oldRdd: RDD[_], newRdd: RDD[_],
                    stat: Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)]): RDD[TablePartition] = {
    logDebug("To zip tachyon rdd")
    val rdd = prepareTablePartitionRDDBasedOnExistingRDD(newRdd, stat, oldRdd)
    rdd.cache()
    if(cachedRDD!=null)cachedRDD.unpersist()
    cachedRDD = rdd
    writeTablePartitionRDDToTachyon(rdd)
  }

  def prepareTablePartitionRDD(inputRdd: RDD[_],
    stat: Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)])
    : RDD[TablePartition] = {
    logDebug("rdd: " + inputRdd.toDebugString)
    inputRdd.mapPartitionsWithIndex { case(partitionIndex, iter) => {
      val tablePartitionBuilder = createPartitionBuilder()

      iter.foreach(row => {
        addRow(tablePartitionBuilder, row, createRecordStructObjectInspector)
      })

      if(stat!= null)stat += Tuple2(partitionIndex, tablePartitionBuilder.stats)
      Iterator(tablePartitionBuilder.build)
    }}
  }

  def prepareTablePartitionRDDBasedOnExistingRDD(inputRdd: RDD[_],
    stat: Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)],
    origRdd: RDD[_])
    : RDD[TablePartition] = {
    //only workable for the same partition number
    if(origRdd.partitions.length != inputRdd.partitions.length )
      throw new Exception("Non-equal partition number between newly input and original persisted RDDs");

    origRdd.asInstanceOf[RDD[TablePartition]].zipPartitions(inputRdd.asInstanceOf[RDD[Any]])(
      (oldRDDIter: Iterator[TablePartition], newRDDIter: Iterator[Any]) => {
        val tablePartitionBuilder = createPartitionBuilder()
        oldRDDIter.foreach(part => {
          part.iterator.foreach( row =>
            addRow(tablePartitionBuilder, row, createColumnarStructObjectInspector)
          ) })
        newRDDIter.foreach(row => addRow(tablePartitionBuilder, row, createRecordStructObjectInspector))
        Iterator(tablePartitionBuilder)
      })mapPartitionsWithIndex{ case(partitionIndex, iter) => {
      val partition = iter.next()
      if(stat!= null) stat += Tuple2(partitionIndex, partition.stats)
      Iterator(partition.build)
    }

    }
  }

  def addRow(tablePartitionBuilder: TablePartitionBuilder ,
                                   row: Any,
                                   objectInspector: StructObjectInspector):TablePartitionBuilder = {
    val soi = objectInspector
    val fields: JList[_ <: StructField] = soi.getAllStructFieldRefs
    val obj =   row.asInstanceOf[Object]

    val currTm = System.currentTimeMillis() / 1000
    val longInspector = PrimitiveObjInspectorFactory.newPrimitiveObjInspector("Long")

    val timeField = fields.get(timeColumnIndex)
    val timeFieldObjInspector = timeField.getFieldObjectInspector
    val recTm = soi.getStructFieldData(obj, timeField)

    val compareWithZero = ObjectInspectorUtils.compare(0L.asInstanceOf[Object], longInspector,
      recTm, timeFieldObjInspector)
    lazy val elapsedTime = ObjectInspectorUtils.copyToStandardJavaObject(
      currTm.asInstanceOf[Object], longInspector).asInstanceOf[Long] -
      ObjectInspectorUtils.copyToStandardJavaObject(
        recTm, timeFieldObjInspector).asInstanceOf[Long]

    if(compareWithZero != 0 && (cleanBefore == -1 || elapsedTime < cleanBefore)) {
      //append single row here
      for (i <- 0 until fields.size) {
        val field = fields.get(i)
        val fieldOI: ObjectInspector = field.getFieldObjectInspector
        fieldOI.getCategory match {
          case ObjectInspector.Category.PRIMITIVE =>
            tablePartitionBuilder.append(i, soi.getStructFieldData(obj, field), fieldOI)
          case other => {
            throw new Exception("Not support NonPrimitiveType currently")
          }
        }
      }
      tablePartitionBuilder.incrementRowCount()
    }
    tablePartitionBuilder
  }

  def createRecordStructObjectInspector :RecordStructObjectInspector =
    new RecordStructObjectInspector(this.fieldNames.toList, this.formats.toList)

  class RecordStructObjectInspector extends StructObjectInspector{
    private var fields: JList[IDStructField] = _

    override def getTypeName(): String =  {
      ObjectInspectorUtils.getStandardStructTypeName(this)
    }

    def this(structFieldNames: JList[String],
             structFieldObjectInspectorsInString: JList[String]) {
      this()
      val structObjectInspectors = new JArrayList[ObjectInspector](structFieldNames.size())
      for(i <- 0 until structFieldNames.size) {
        val pType = new PrimitiveTypeInfo
        pType.setTypeName(structFieldObjectInspectorsInString.get(i))
        val fieldOI =  pType.getCategory match {
          case Category.PRIMITIVE =>  PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
            pType.getPrimitiveCategory)
          case _ => throw new Exception("Not support NonPrimitiveType")
        }
        structObjectInspectors.add(fieldOI)
      }
      init(structFieldNames, structObjectInspectors, null)
    }

    protected def init(structFieldNames: JList[String] ,
                       structFieldObjectInspectors: JList[ObjectInspector] ,
                       structFieldComments: JList[String]) {
      assert (structFieldNames.size() == structFieldObjectInspectors.size())
      assert (structFieldComments == null ||
        (structFieldNames.size() == structFieldComments.size()))

      fields = new JArrayList[IDStructField](structFieldNames.size())
      for (i <- 0 until structFieldNames.size()) {
        fields.add(new IDStructField(i, structFieldNames.get(i),
          structFieldObjectInspectors.get(i),
          if(structFieldComments == null)null else structFieldComments.get(i)))
      }
    }

    override def getCategory(): Category = {
      Category.STRUCT
    }

    // Without Data
    override def getStructFieldRef(fieldName: String): StructField =  {
      ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields)
    }


    override def getAllStructFieldRefs: JList[_ <: StructField] = {
      this.fields
    }

    override def getStructFieldData(data: Object,  fieldRef: StructField):Object =  {
      val (t1, ((t2, t3), t4)) = data.asInstanceOf[(Object, ((Object, Object), Object))]
      this.fields.map(_.getFieldName).zip(Array(t1, t2, t3, t4)).toMap
        .apply(fieldRef.getFieldName)
    }

    override def getStructFieldsDataAsList(data: Object):JList[Object] = {
      val (t1, ((t2, t3), t4)) = data.asInstanceOf[(Object, ((Object, Object), Object))]
      Array(t1, t2, t3, t4).toList
    }
  }

  def createColumnarStructObjectInspector: ColumnarStructObjectInspector = {
    val columnNames = this.fieldNames.toList
    val columnTypes = this.formats.map(s=> {
      val pType = new PrimitiveTypeInfo
      pType.setTypeName(s.toLowerCase)
      pType
    }).toList

    val fields = new JArrayList[StructField]()
    for (i <- 0 until columnNames.size) {
      val typeInfo = columnTypes.get(i)
      val fieldOI = typeInfo.getCategory match {
        case Category.PRIMITIVE => PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          typeInfo.asInstanceOf[PrimitiveTypeInfo].getPrimitiveCategory)
        case _ => throw new Exception("Not support NonPrimitiveType")
      }
      fields.add(new IDStructField(i, columnNames.get(i), fieldOI))
    }
    new ColumnarStructObjectInspector(fields)
  }

  def createPartitionBuilder(shouldCompress: Boolean = false): TablePartitionBuilder =
    new TablePartitionBuilder(createColumnarStructObjectInspector, COLUMN_SIZE,
      shouldCompress)

  /**
   * Write the RDD onto Tachyon file system
   * @param inputrdd
   * @return
   */
  def writeTablePartitionRDDToTachyon(inputrdd: RDD[TablePartition]): RDD[TablePartition] = {
    // Put the table in Tachyon.
    logDebug("Putting RDD for %s in Tachyon".format(tableKey))

    if(!SharkEnv.tachyonUtil.tableExists(tableKey, None)) {
      tachyonWriter.createTable(ByteBuffer.allocate(0))
    }

    inputrdd.mapPartitionsWithIndex {
      case(partitionIndex, iter) => {
        val partition = iter.next()
        writeColumnPartition(partition, partitionIndex)
        Iterator(partition)
      }
    }
  }

  def writeColumnPartition(partition: TablePartition, partitionIndex: Int) {
    partition.toTachyon.zipWithIndex.foreach { case(buf, column) =>
      val  f: Future[Unit] = future {
        //write output to tachyon in parallel
        //TODO to cache the exception and register some onSuccess or onFailure actions.

        // we'd better to use the existing shark's API, but unfortunately the following function cannot overwrite the
        // existing partition files. Since the slave side cannot get rawTable without calling createTable()
        //tachyonWriter.writeColumnPartition(column, partitionIndex, buf)

        //the workaround solution
        val tablepath = SharkEnvSlave.tachyonUtil.warehousePath + "/" + tableKey
        val rawTable = SharkEnvSlave.tachyonUtil.client.getRawTable(tablepath)
        val rawColumn = rawTable.getRawColumn(column)
        var file = rawColumn.getPartition(partitionIndex)
        if(file!=null && SharkEnvSlave.tachyonUtil.client.exist(file.getPath)) {
          //to delete the existing partition file before hand
          //TODO this solution will cause shark query failure!!!!
          SharkEnvSlave.tachyonUtil.client.delete(file.getPath, true)
        }
        rawColumn.createPartition(partitionIndex)
        file = rawColumn.getPartition(partitionIndex)
        //TODO catch exception when caching data
        val outStream = file.getOutStream(WriteType.CACHE_THROUGH)
        try{
          outStream.write(buf.array(), 0, buf.limit())
        }
        finally {
          //to close the outStream in any case
          if(outStream != null) outStream.close()
        }
      }}
  }
}

package stream.framework.output

import stream.framework.output.column._

import java.nio.ByteBuffer
import java.nio.ByteOrder

import spark.streaming.DStream

import tachyon.client.OpType

import org.apache.hadoop.io.Text

class TachyonStrToLongOutput extends AbstractTachyonEventOutput {
  
  @transient lazy val table = getTachyonTable(4)
  
  override def output(stream: DStream[_]) {
    stream.mapPartitionsWithTimeIndexId((time, index, id, iter) => {
      val colBuilders = Array(new LongColumnBuilder, new StringColumnBuilder, 
          new LongColumnBuilder)
      colBuilders.foreach(_.initialize(100))
      
      val partition = tryGetUniqPartition(hashCode(time, index, id))
      println(">>>>> partition id:" + partition)
      
      val currTime = time / 1000
      var count = 0
      iter.foreach(r => {
        count += 1       
        val newRow = r.asInstanceOf[(String, Long)]
        
        colBuilders(0).asInstanceOf[LongColumnBuilder].append(currTime)
        colBuilders(1).asInstanceOf[StringColumnBuilder].append(new Text(newRow._1))
        colBuilders(2).asInstanceOf[LongColumnBuilder].append(newRow._2)
      })
      
      //write to partition
      colBuilders.zipWithIndex.foreach(c => {
        val col = table.getRawColumn(c._2 + 1)
        col.createPartition(partition)
        
        val file = col.getPartition(partition)
        val os = file.createOutStream(OpType.WRITE_CACHE)
        
        val buf = c._1.build
        os.write(buf)
        os.close()
      })
      
      createMetaCol(count, partition)
      
      iter
    }).foreach(r => r.foreach(_ => Unit))
  }
  
  private def hashCode(millisecond: Long, index: Int, id: Long) = 
    (41 * (41 * (41 * millisecond) + index) + id).toInt
    
  private def tryGetUniqPartition(part: Int) = {
    var tmpPart = part
    val col = table.getRawColumn(0)
    while (col.getPartition(tmpPart) != null) {
      tmpPart += 1
    }
    
    tmpPart
  }
    
  private def createMetaCol(rowNum: Long, part: Int) {
    val metaCol = table.getRawColumn(0)
    metaCol.createPartition(part)
    
    val file = metaCol.getPartition(part)
    val os = file.createOutStream(OpType.WRITE_CACHE)
    
    val buf = ByteBuffer.allocate(8)
    buf.order(ByteOrder.nativeOrder).putLong(rowNum).flip()
    
    os.write(buf)
    os.close()
  }
}

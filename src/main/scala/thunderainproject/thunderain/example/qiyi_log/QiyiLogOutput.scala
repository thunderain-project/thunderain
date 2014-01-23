package thunderainproject.thunderain.example.qiyi_log

import org.apache.spark.SparkContext._
import org.apache.spark.streaming.dstream.DStream

import shark.SharkEnv

import thunderainproject.thunderain.framework.output.AbstractEventOutput

class QiyiLogOutput extends AbstractEventOutput {
  override def output(stream: DStream[_]) {
    stream.asInstanceOf[DStream[Array[Long]]] foreach { (r, t) =>
      val totalCount = SharkEnv.sc.accumulator(0l)
      val totalTimeLen = SharkEnv.sc.accumulator(0l)

      r foreach { v =>
        totalCount += v(0)
        totalTimeLen += v(1)
      }

      println(t.milliseconds + " " + outputName + ": " +
        totalCount.value + " " + totalTimeLen.value)
    }
  }

}

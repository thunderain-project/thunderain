package stream.framework.operator

import spark.streaming.DStream
import spark.streaming.StreamingContext._
import spark.streaming.Seconds

import stream.framework.Event

class CountOperator(
    val key: String,
    val hierarchy: Option[String] = Some("/"),
    val window: (Option[Long], Option[Long]) = (None, None))
    extends AbstractOperator with Serializable {

  override def process(stream: DStream[Event]) = {
    val windowedStream = windowStream(stream, window)
    
    windowedStream.flatMap(r => hierarchyString(r.keyMap(key), hierarchy))
      .countByValue()
  }
}
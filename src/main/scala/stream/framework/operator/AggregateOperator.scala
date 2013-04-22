package stream.framework.operator

import spark.streaming.DStream
import spark.streaming.Duration
import spark.streaming.StreamingContext._

import stream.framework.Event

class AggregateOperator(
    val key: String,
    val value: String,
    val hierarchy: Option[String] = Some("/"),
    val window: (Option[Long], Option[Long]) = (None, None)
    ) extends AbstractOperator with Serializable {

  override def process(stream: DStream[Event]) = {
    val windowedStream = windowStream(stream, window)
    
    windowedStream.flatMap(r => 
      hierarchyString(r.keyMap(key), hierarchy).map(s => (s, r.keyMap(value))))
      .groupByKey()
  }
}
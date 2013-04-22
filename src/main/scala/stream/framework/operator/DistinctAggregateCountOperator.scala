package stream.framework.operator

import spark.streaming.DStream
import spark.streaming.StreamingContext._

import stream.framework.Event

class DistinctAggregateCountOperator (
    val key: String,
    val value: String,
    val hierarchy: Option[String] = Some("/"),
    val window: (Option[Long], Option[Long]) = (None, None))
  extends AbstractOperator with Serializable {
  
  override def process(stream: DStream[Event]) = {
    val windowedStream = windowStream(stream, window)
    
    // distinct
    // (K1, V1) => ((K1, V1), 1)
    // ((K1, V1), 1) => ((K1, V1), 1)
    // count
    // ((K1, V1), 1) => (K1, 1)
    // (K1, 1) => (K1, SUM)
    windowedStream.flatMap(r =>
      hierarchyString(r.keyMap(key), hierarchy).map(
          s => ((s, r.keyMap(value)), 1)))
      .reduceByKey((a, b) => a)
      .map(_._1._1)
      .countByValue()
  }
}
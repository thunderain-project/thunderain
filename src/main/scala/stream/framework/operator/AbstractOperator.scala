package stream.framework.operator

import scala.collection.mutable

import spark.streaming.DStream
import spark.streaming.Seconds

import stream.framework.Event

abstract class AbstractOperator {
  def process(stream: DStream[Event])
  
  def windowStream[U: ClassManifest](stream: DStream[U],
		  window: (Option[Long], Option[Long])) = {  
    window match {
    case (Some(a), Some(b)) =>
      stream.window(Seconds(a), Seconds(b))
    case (Some(a), None) =>
      stream.window(Seconds(a))
    case _ =>
      stream
    }
  }
  
}
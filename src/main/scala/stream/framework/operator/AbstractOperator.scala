package stream.framework.operator

import scala.collection.mutable

import spark.streaming.DStream
import spark.streaming.Seconds

import stream.framework.Event

abstract class AbstractOperator {
  def process(stream: DStream[Event]): DStream[_]
  
  def windowStream[U: ClassManifest](stream: DStream[U],
		  window: (Option[Long], Option[Long])) = {  
    window match {
    case (a: Some[Long], b: Some[Long]) =>
      stream.window(Seconds(a.get), Seconds(b.get))
    case (a: Some[Long], None) =>
      stream.window(Seconds(a.get))
    case _ =>
      stream
    }
  }
  
  def hierarchyString(s: String, delimiter: Option[String]) = {
    delimiter match {
      case Some(t) => s.split(t).scanLeft("")(_ + _).drop(1)
      case None => Array(s)
    }
  }
}
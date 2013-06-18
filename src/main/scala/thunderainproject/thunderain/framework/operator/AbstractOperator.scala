package thunderainproject.thunderain.framework.operator

import spark.streaming.{DStream, Seconds}

import thunderainproject.thunderain.framework.Event

abstract class AbstractOperator extends Serializable {
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
package stream.framework.operator

import spark.streaming.DStream

import stream.framework.Event

class NoneOperator extends AbstractOperator with Serializable {
  override def process(stream: DStream[Event]) = stream
}
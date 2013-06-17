package thunderainproject.thunderain.framework.operator

import scala.xml._

import spark.streaming.DStream

import thunderainproject.thunderain.framework.Event


class NoneOperator extends AbstractOperator with Serializable with OperatorConfig {
  override def parseConfig(conf: Node) {}
  override def process(stream: DStream[Event]) {}
}
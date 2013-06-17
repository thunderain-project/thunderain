package thunderainproject.thunderain.framework.operator

import scala.xml.Node

trait OperatorConfig {
  def parseConfig(conf: Node)
}
package thunderainproject.thunderain.framework.operator

import scala.xml._

import spark.streaming.DStream
import spark.streaming.StreamingContext._

import thunderainproject.thunderain.framework.Event
import thunderainproject.thunderain.framework.output.AbstractEventOutput

class CountOperator extends AbstractOperator with OperatorConfig {
  
  class CountOperatorConfig (
    val name: String,
    val window: Option[Long],
    val slide: Option[Long],
    val key: String,
    val outputClsName: String) extends Serializable 
  
  override def parseConfig(conf: Node) {  
    val nam = (conf \ "@name").text
    
    val propNames = Array("@window", "@slide")
    val props = propNames.map(p => {
      val node = conf \ "property" \ p
      if (node.length == 0) {
        None
      } else {
        Some(node.text)
      }
    })
    
    val key = (conf \ "key").text
    val output = (conf \ "output").text
    
    config = new CountOperatorConfig(
        nam, 
        props(0) map { s => s.toLong },
        props(1) map { s => s.toLong },
        key,
        output)
    
    outputCls = try {
      Class.forName(config.outputClsName).newInstance().asInstanceOf[AbstractEventOutput]
    } catch {
      case e: Exception => throw new Exception("class " + config.outputClsName + " new instance failed")
    }
    outputCls.setOutputName(config.name)
  }
  
  protected var config: CountOperatorConfig = _
  protected var outputCls: AbstractEventOutput = _
  
  override def process(stream: DStream[Event]) {
    val windowedStream = windowStream(stream, (config.window, config.slide))
    val resultStream = windowedStream.map(r => r.keyMap(config.key)).countByValue()
    
    outputCls.output(outputCls.preprocessOutput(resultStream))
  }
}
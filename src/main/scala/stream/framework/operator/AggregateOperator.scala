package stream.framework.operator

import scala.xml._

import spark.streaming.DStream
import spark.streaming.Duration
import spark.streaming.StreamingContext._

import stream.framework.Event
import stream.framework.output.AbstractEventOutput

class AggregateOperator extends AbstractOperator with Serializable with OperatorConfig {
  class AggregateOperatorConfig (
    val name: String,
    val window: Option[Long],
    val slide: Option[Long],
    val key: String,
    val value: String,
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
    val value = (conf \ "value").text
    val output = (conf \ "output").text
    
    config = new AggregateOperatorConfig(
        nam, 
        props(0) match {case Some(s) => Some(s.toLong); case None => None},
        props(1) match {case Some(s) => Some(s.toLong); case None => None},
        key,
        value,
        output)
    
    outputCls = try {
      Class.forName(config.outputClsName).newInstance().asInstanceOf[AbstractEventOutput]
    } catch {
      case _ => throw new Exception("class" + config.outputClsName + " new instance failed")
    }
    outputCls.setOutputName(config.name)
  }
  
  protected var config: AggregateOperatorConfig = _
  protected var outputCls: AbstractEventOutput = _
  
  override def process(stream: DStream[Event]) {
    val windowedStream = windowStream(stream, (config.window, config.slide)) 
    val resultStream = windowedStream.map(r => (r.keyMap(config.key), r.keyMap(config.value))).groupByKey()
    
    outputCls.output(outputCls.preprocessOutput(resultStream))
  }
}
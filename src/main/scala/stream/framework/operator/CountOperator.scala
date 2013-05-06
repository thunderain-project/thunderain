package stream.framework.operator

import scala.xml._

import spark.streaming.DStream
import spark.streaming.StreamingContext._
import spark.streaming.Seconds

import stream.framework.Event
import stream.framework.output.AbstractEventOutput

class CountOperator extends AbstractOperator with Serializable with OperatorConfig {
  
  class CountOperatorConfig (
    val name: String,
    val window: Option[Long],
    val slide: Option[Long],
    val hierarchy: Option[Boolean],
    val key: String,
    val outputClsName: String) extends Serializable 
  
  override def parseConfig(conf: Node) {  
    val nam = (conf \ "@name").text
    
    val propNames = Array("@window", "@slide", "@hierarchy")
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
        props(0) match {case Some(s) => Some(s.toLong); case None => None},
        props(1) match {case Some(s) => Some(s.toLong); case None => None},
        props(2) match {case Some(s) => Some(s.toBoolean); case None => None},
        key,
        output)
    
    outputCls = try {
      Class.forName(config.outputClsName).newInstance().asInstanceOf[AbstractEventOutput]
    } catch {
      case _ => throw new Exception("class" + config.outputClsName + " new instance failed")
    }
    outputCls.setOutputName(config.name)
  }
  
  private var config: CountOperatorConfig = _
  private var outputCls: AbstractEventOutput = _
  
  override def process(stream: DStream[Event], f: String => Array[String] = null) {
    val windowedStream = windowStream(stream, (config.window, config.slide))
    
    val hierarStream = if (f != null) {
      windowedStream.flatMap(r => f(r.keyMap(config.key)))
    } else {
      windowedStream.map(r => r.keyMap(config.key))
    }
    
    outputCls.output(hierarStream.countByValue())
  }
}
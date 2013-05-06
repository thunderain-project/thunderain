package stream.framework.operator

import scala.xml._

import spark.streaming.DStream
import spark.streaming.StreamingContext._

import stream.framework.Event
import stream.framework.output.AbstractEventOutput

class DistinctAggregateCountOperator 
extends AbstractOperator with Serializable with OperatorConfig {
    class DACOperatorConfig(
    val name: String,
    val window: Option[Long],
    val slide: Option[Long],
    val hierarchy: Option[Boolean],
    val key: String,
    val value: String,
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
    val value = (conf \ "value").text
    val output = (conf \ "output").text
    
    config = new DACOperatorConfig(
        nam, 
        props(0) match {case Some(s) => Some(s.toLong); case None => None},
        props(1) match {case Some(s) => Some(s.toLong); case None => None},
        props(2) match {case Some(s) => Some(s.toBoolean); case None => None},
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
  
  private var config: DACOperatorConfig = _
  private var outputCls: AbstractEventOutput = _
  
  override def process(stream: DStream[Event], f: String => Array[String] = null) {
    val windowedStream = windowStream(stream, (config.window, config.slide))
    
    // distinct
    // (K1, V1) => ((K1, V1), 1)
    // ((K1, V1), 1) => ((K1, V1), 1)
    // count
    // ((K1, V1), 1) => (K1, 1)
    // (K1, 1) => (K1, SUM)
    val hierarStream = if (f != null) {
      windowedStream.flatMap(
          r => f(r.keyMap(config.key)).map(s => ((s, r.keyMap(config.value)), 1)))
    } else {
      windowedStream.map(r => ((r.keyMap(config.key), r.keyMap(config.value)), 1))
    }
    
    outputCls.output(hierarStream.reduceByKey((a, b) => a).map(_._1._1).countByValue())
  }
}
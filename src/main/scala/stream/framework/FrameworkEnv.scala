package stream.framework

import spark.streaming.DStream

import stream.framework.parser.AbstractEventParser
import stream.framework.output.AbstractEventOutput
import stream.framework.output.StdEventOutput
import stream.framework.operator._
import stream.framework.util._

object FrameworkEnv {

  class AppEnv(val appConfig: AppConfig) extends Serializable {
    
    // notice. appConfig, parser, output should be serializable, it will
    // be deserialized by spark streaming checkpoint, if add @transient,
    // after deserialize, these fields will be null
    val parser = try {
      Class.forName(appConfig.parserClass).newInstance().asInstanceOf[AbstractEventParser]
    } catch {
      case e: Exception => println(e.getStackTraceString); exit(1)
    }
    
    val operators = appConfig.properties.map(p => {
      val op = p match {
        case s: CountProperty => 
          new CountOperator(s.key, s.hierarchy, s.window)
        case s: AggregateProperty => 
          new AggregateOperator(s.key, s.value, s.hierarchy, s.window)
        case s: DistinctAggregateCountProperty =>
          new DistinctAggregateCountOperator(s.key, s.value, s.hierarchy, s.window)
        case _ => new NoneOperator
      }
      
      val output = try {
        Class.forName(p.outputClass).newInstance().asInstanceOf[AbstractEventOutput]
      } catch {
        case _ => throw new Exception("class " + p.outputClass + " new instance failed")
      }
      output.setOutputName(p.name)
      (op, output)
    })
    
    def process(stream: DStream[String]) {
      val eventStream = stream.map(s => parser.parseEvent(s, appConfig.schemas))
      operators.foreach(p => {
        val resultStream = p._1.process(eventStream)
        p._2.output(resultStream)
        })
    }
  }
}
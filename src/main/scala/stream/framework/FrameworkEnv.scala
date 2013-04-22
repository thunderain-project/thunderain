package stream.framework

import spark.streaming.DStream
import spark.streaming.StreamingContext
import spark.streaming.Seconds
import spark.SparkContext

import stream.framework.util.AppConfig
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
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("FrameworkEnv conf/properties.xml")
      exit(1)
    }
    
    System.setProperty("spark.cleaner.ttl", "3600")
    
    //parse the conf file
    Configuration.parseConfig(args(0))

    //for each app, create its app env
    val appEnvs = Configuration.appConfMap.map(e => (e._1, new AppEnv(e._2)))
    
    //create kafka input stream    
    val master = System.getenv("SPARK_MASTER_URL")
    val sparkHome = System.getenv("SPARK_HOME")
    val jars = System.getenv("STREAM_JAR_PATH").split(":")
    
    val sc = new SparkContext(master, "kafkaStream", sparkHome, jars.toSeq)
    val ssc =  new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("checkpoint")
    
    val zkQuorum = System.getenv("ZK_QUORUM")
    val group = System.getenv("KAFKA_GROUP")
    /****************TODO. this should be modified later*******************/
    //cause all the topics are in one DStream, first we should filter out
    // what topics to what application
    // because kafka stream currently do not support decode method
    // other than string decode, so currently workaround solution is:
    // all the input message should follow this format: "category|||message",
    // "|||" is a delimiter, category is topic name, message is content
    val lines = ssc.kafkaStream[String](zkQuorum, group, 
         Configuration.appConfMap.map(e => (e._1, 1)))
    val streams = appEnvs.map(e => (e._1, lines.filter(s => s.startsWith(e._1))))
    	 .map(e => (e._1, e._2.map(s => s.substring(s.indexOf("|||") + 3))))
    
    streams.foreach(e => appEnvs(e._1).process(e._2))
    
    
    ssc.start()
  }
}
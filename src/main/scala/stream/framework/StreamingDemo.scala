package stream.framework

import org.apache.log4j.PropertyConfigurator

import shark.SharkServer
import shark.SharkEnv

import spark.SparkEnv
import spark.streaming.StreamingContext
import spark.streaming.Seconds

object StreamingDemo {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("StreamingDemo conf/properties.xml conf/log4j.properties")
      System.exit(1)
    }
    
    System.setProperty("spark.cleaner.ttl", "3600")
    System.setProperty("spark.stream.concurrentJobs", "2")
    
    var sparkEnv: SparkEnv = null
    //start shark server thread
    val sharkThread = new Thread("SharkServer") {
      setDaemon(true)
      override def run() {
        SharkEnv.initWithSharkContext("Streaming Demo with Shark")
        sparkEnv = SparkEnv.get
        SharkServer.main(Array())
      }
    }
    sharkThread.start()
    Thread.sleep(10000)
    
    PropertyConfigurator.configure(args(1))
    
    //parse the conf file
    FrameworkEnv.parseConfig(args(0))
    
    //create streaming context
    SparkEnv.set(sparkEnv)
    val ssc =  new StreamingContext(SharkEnv.sc, Seconds(10))
    ssc.checkpoint("checkpoint")

    //register exit hook
    Runtime.getRuntime().addShutdownHook(
      new Thread() {
        override def run() {
          println("streaming context stopped")
          ssc.stop()
        }
      })
    
    val zkQuorum = System.getenv("ZK_QUORUM")
    val group = System.getenv("KAFKA_GROUP")
    val apps = FrameworkEnv.apps
    /****************TODO. this should be modified later*******************/
    //cause all the topics are in one DStream, first we should filter out
    // what topics to what application
    // because kafka stream currently do not support decode method
    // other than string decode, so currently workaround solution is:
    // all the input message should follow this format: "category|||message",
    // "|||" is a delimiter, category is topic name, message is content
    val kafkaInputs = System.getenv("KAFKA_INPUT_NUM").toInt
    val lines = (1 to kafkaInputs).map(_ => 
      ssc.kafkaStream[String](zkQuorum, group, apps.map(e => (e._1, 1))))
      .toArray
    val union = ssc.union(lines)
         
    val streams = apps.map(e => (e._1, union.filter(s => s.startsWith(e._1))))
    	 .map(e => (e._1, e._2.map(s => s.substring(s.indexOf("|||") + 3))))
    
    streams.foreach(e => apps(e._1).process(e._2))
    
    ssc.start()
  }
}
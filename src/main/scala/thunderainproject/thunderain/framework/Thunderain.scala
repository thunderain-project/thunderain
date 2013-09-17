/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package thunderainproject.thunderain.framework

import org.apache.log4j.PropertyConfigurator

import org.apache.spark.SparkEnv
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

import shark.{SharkEnv, SharkServer}


object Thunderain {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Thunderain conf/properties.xml conf/log4j.properties [conf/fairscheduler.xml]")
      System.exit(1)
    }

    System.setProperty("spark.cleaner.ttl", "600")
    System.setProperty("spark.stream.concurrentJobs", "2")

    val schedulerEnabled = if (args.length > 2) {
      System.setProperty("spark.scheduler.mode", "FAIR")
      System.setProperty("spark.scheduler.allocation.file", args(2))
      true
    } else {
      false
    }

    var sparkEnv: SparkEnv = null
    //start shark server thread
    val sharkThread = new Thread("SharkServer") {
      setDaemon(true)
      override def run() {
        SharkEnv.initWithSharkContext("Thunderain")
        if (schedulerEnabled == true) {
          SharkEnv.sc.setLocalProperty("spark.scheduler.pool", "1")
        }
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
    if (schedulerEnabled == true) {
      ssc.sparkContext.setLocalProperty("spark.scheduler.pool", "2")
    }

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
    // because all the topics are in one DStream, first we should filter out
    // what topics to what application
    // because kafka stream currently do not support decode method
    // other than string decode, so currently workaround solution is:
    // all the input message should follow this format: "category|||message",
    // "|||" is a delimiter, category is topic name, message is content
    val kafkaInputs = System.getenv("KAFKA_INPUT_NUM").toInt
    val lines = (1 to kafkaInputs).map(_ =>
      ssc.kafkaStream(zkQuorum, group, apps.map(e => (e._1, 1)))).toArray
    val union = ssc.union(lines)

    val streams = apps.map(e => (e._1, union.filter(s => s.startsWith(e._1))))
    	 .map(e => (e._1, e._2.map(s => s.substring(s.indexOf("|||") + 3))))

    streams.foreach(e => apps(e._1).process(e._2))

    ssc.start()
  }
}

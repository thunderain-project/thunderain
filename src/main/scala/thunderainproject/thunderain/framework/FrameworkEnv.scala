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

import org.apache.spark.streaming.dstream.DStream

import scala.xml._
import scala.collection.mutable

import parser.AbstractEventParser
import operator._

object FrameworkEnv {
  //hash map for all operators in one framework
  val operators = new mutable.HashMap[String, String]

  //hash map for all applications in one framework
  val apps = new mutable.HashMap[String, App]

  def parseConfig(xmlFilePath: String) {
    val xmlFile = XML.load(xmlFilePath)

    (xmlFile \ "operators" \ "operator").foreach(n => {
      operators.put((n \ "@type").text, (n \ "@class").text)
    })

    // parse each application
    xmlFile match {
      case <applications>{allApps @ _*}</applications> =>
        for (appConf @ <application>{_*}</application> <- allApps) {
          val app = new App(appConf)
          apps += ((app.category, app))

        }
    }
  }

  class App(conf: Node) extends Serializable {
    var category: String = _
    private var parser: AbstractEventParser = _
    private var items: Array[String] = _

    val jobs = new mutable.HashMap[String, AbstractOperator]

    parseAppConf(conf)

    def parseAppConf(conf: Node) {
      category = (conf \ "category").text

      val parserCls = (conf \ "parser").text
      parser = try {
        Class.forName(parserCls).newInstance().asInstanceOf[AbstractEventParser]
      } catch {
        case e: Exception => println(e.getStackTraceString); exit(-1)
      }

      items = (conf \ "items" \ "item").map(_.text).toArray

      (conf \ "jobs" \ "job").foreach(j => {
        val name = (j \ "@name").text
        val typ = (j \ "@type").text

        val op = operators.get(typ) match {
          case Some(s) => Class.forName(s).newInstance().asInstanceOf[OperatorConfig]
          case None => throw new Exception("type " + typ + " cannot find related operator")
        }
        op.parseConfig(j)

        jobs.put(name, op.asInstanceOf[AbstractOperator])
      })
    }

    def process(stream: DStream[String]) {
      val eventStream = stream.map(s => parser.parseEvent(s, items))

      jobs.foreach(j => j._2.process(eventStream))
    }
  }
}

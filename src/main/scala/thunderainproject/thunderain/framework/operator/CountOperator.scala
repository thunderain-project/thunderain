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

package thunderainproject.thunderain.framework.operator

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._

import scala.xml._

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

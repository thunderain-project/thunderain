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

class DistinctAggregateCountOperator
extends AbstractOperator with OperatorConfig {
    class DACOperatorConfig(
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

    config = new DACOperatorConfig(
        nam,
        props(0) map { s => s.toLong },
        props(1) map { s => s.toLong },
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

  protected var config: DACOperatorConfig = _
  protected var outputCls: AbstractEventOutput = _

  override def process(stream: DStream[Event]) {
    val windowedStream = windowStream(stream, (config.window, config.slide))

    // distinct
    // (K1, V1) => ((K1, V1), 1)
    // ((K1, V1), 1) => ((K1, V1), 1)
    // count
    // ((K1, V1), 1) => (K1, 1)
    // (K1, 1) => (K1, SUM)
    val resultStream = windowedStream.
      map(r => ((r.keyMap(config.key), r.keyMap(config.value)), 1)).
      reduceByKey((a, b) => a).map(_._1._1).countByValue()

    outputCls.output(outputCls.preprocessOutput(resultStream))
  }
}

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

package thunderainproject.thunderain.example.weblog.operator

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkContext._

import shark.{SharkEnv, SharkContext}

import thunderainproject.thunderain.framework.Event
import thunderainproject.thunderain.framework.operator.CountOperator

class JoinCountOperator extends CountOperator {
  @transient lazy val itemCategoryRdd = SharkEnv.sc.asInstanceOf[SharkContext]
    .sql2rdd(
      "SELECT i_item_sk, i_category FROM item_ext")
    .map(r => (r.getLong(0).toLong, r.getString(1)))
    .cache

  override def process(stream: DStream[Event]) {
    val windowedStream = windowStream(stream, (config.window, config.slide))
    val itemStream = windowedStream.map(e =>
      (e.keyMap(config.key).toLong, e.keyMap(config.key).toLong))
      .transform(_.join(itemCategoryRdd))

    val resultStream = config.name match {
      case "item_view" =>
        itemStream.map(r => (r._2._2, r._2._1)).countByValue()
      case "subcategory_view" =>
        itemStream.map(r => (r._2._2, r._2._2)).countByValue()
      case _ => throw new Exception("unknown job name " + config.name)
    }

    outputCls.output(outputCls.preprocessOutput(resultStream))
  }

  def splitCategory(category: String) = {
    val sb = new StringBuilder()
    val newCategory = if (category.startsWith("/")) {
      category
    } else {
      "/" + category
    }
    val splits = newCategory.split("/")

    splits.zipWithIndex.map(r => {
      sb.append(r._1 + "/")
      val subcategory = if (r._2 + 1 >= splits.length){
        "Nil"
      } else {
        splits(r._2 + 1)
      }
      (sb.toString, subcategory)
    })
  }
}

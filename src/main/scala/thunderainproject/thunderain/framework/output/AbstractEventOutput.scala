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

package thunderainproject.thunderain.framework.output

import spark.streaming.DStream

abstract class AbstractEventOutput extends Serializable{
  protected var formats: Array[String] = _
  protected var outputName: String = _
  
  /**
   * abstract method of output DStream, derivatives should implement this.
   */
  def output(stream: DStream[_])
  
  /**
   * set the output name, derivatives can use it to set output name.
   */
  def setOutputName(name: String) {
    outputName = name
  }
  
  /**
   * set the output data format, derivatives can use it to set formats.
   */
  def setOutputDataFormat(formats: Array[String]) {
    this.formats = formats
  }
  
  /**
   * preprocess the output stream, derivatives can override this.
   */
  def preprocessOutput(stream: DStream[_]): DStream[_] = {
    stream
  }
}

class StdEventOutput extends AbstractEventOutput {
  override def output(stream: DStream[_]) {
    stream.print()
  }
}
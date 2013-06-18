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

package thunderainproject.thunderain.example.weblog

import thunderainproject.thunderain.framework.Event
import thunderainproject.thunderain.framework.parser.AbstractEventParser

class WebLogParser extends AbstractEventParser {
  override def parseEvent(event: String, schema: Array[String]) = {
    val filter = "(.*) - - (.*) \\[(.*)\\] \".*item=(\\d+).*\".* (http.*) \"(.*)\"".r
    
    try {
    	val filter(params @ _*) = event
    	new Event(System.currentTimeMillis() / 1000, schema.zip(params).toMap)
    } catch {
      case _ => new Event(System.currentTimeMillis()/ 1000, schema.map(s => "").zip(schema).toMap)
    }
  }
}

object WebLogParser {
  def main(args: Array[String]) {
    val log = "0.12.189.196 - - 3D4BEC9E3F255FA382AB58DB324D9AAE " +
    		"[14/Dec/1999 00:00:31 -0] \"GET ?item=7749 HTTP/1.1\" " +
    		"500 827 http://www.foo.com?item=7749 \"Mozilla/5.0 (X11; U; FreeBSD; " +
    		"i386; en-US; rv:1.7) Gecko\""
    
    val parser = new WebLogParser
    
    val schemas = Array("sourcep_ip", "cookie_id", "visit_date", "item_id", "referrer", "agent")
    val ev = parser.parseEvent(log, schemas)
    
    println(ev)
  }
}
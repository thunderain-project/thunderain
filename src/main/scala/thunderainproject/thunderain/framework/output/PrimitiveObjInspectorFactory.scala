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

import scala.collection.mutable

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

object PrimitiveObjInspectorFactory {
  val objInspectors = new mutable.HashMap[String, ObjectInspector]
  
  def newPrimitiveObjInspector(primitiveType: String) = {
    objInspectors.getOrElseUpdate(primitiveType, {
      primitiveType match {
        case "Boolean" =>
          PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
        case "Byte" =>
          PrimitiveObjectInspectorFactory.javaByteObjectInspector
        case "Short" =>
          PrimitiveObjectInspectorFactory.javaShortObjectInspector
        case "Int" =>
          PrimitiveObjectInspectorFactory.javaIntObjectInspector
        case "Long" =>
          PrimitiveObjectInspectorFactory.javaLongObjectInspector
        case "Float" =>
          PrimitiveObjectInspectorFactory.javaFloatObjectInspector
        case "Double" =>
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
        case "String" =>
          PrimitiveObjectInspectorFactory.javaStringObjectInspector
        case _ =>
          throw new Exception("unknow primitive type: " + primitiveType)
          
      }
    })
  }
  
}
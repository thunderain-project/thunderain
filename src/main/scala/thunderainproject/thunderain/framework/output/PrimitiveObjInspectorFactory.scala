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
package thunderainproject.thunderain.framework.output

import org.apache.hadoop.io._

object WritableObjectConvertFactory {

  def writableObjectConvert(obj: Object, typ: String): Object = {
    typ match {
      case "Boolean" =>
        obj.asInstanceOf[BooleanWritable].get.asInstanceOf[Object]
      case "Byte" =>
        obj.asInstanceOf[ByteWritable].get.asInstanceOf[Object]
      case "Short" =>
        obj.asInstanceOf[ShortWritable].get.asInstanceOf[Object]
      case "Int" =>
        obj.asInstanceOf[IntWritable].get.asInstanceOf[Object]
      case "Long" =>
        obj.asInstanceOf[LongWritable].get.asInstanceOf[Object]
      case "Float" =>
        obj.asInstanceOf[FloatWritable].get.asInstanceOf[Object]
      case "Double" =>
        obj.asInstanceOf[DoubleWritable].get.asInstanceOf[Object]
      case "String" =>
        obj.asInstanceOf[Text].toString.asInstanceOf[Object]
      case _ =>
        throw new Exception("unknow primitive type: " + typ)
    }
  }
}
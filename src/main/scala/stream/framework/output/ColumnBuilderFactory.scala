package stream.framework.output

import shark.memstore.Column._
import shark.memstore.UncompressedColumnFormat._
import shark.memstore.ColumnNoStats
import shark.memstore.ColumnStats._

object ColumnBuilderFactory {
  val COLUMN_SIZE = 100
  
  def newColumnBuilder(primitiveType: String) = {
    primitiveType match {
      case "Boolean" => 
        new BooleanColumnBuilder(new BooleanColumnFormat(COLUMN_SIZE), BooleanColumnNoStats)
      case "Byte" => 
        new ByteColumnBuilder(new ByteColumnFormat(COLUMN_SIZE), ByteColumnNoStats)
      case "Short" => 
        new ShortColumnBuilder(new ShortColumnFormat(COLUMN_SIZE), ShortColumnNoStats)
      case "Int" => 
        new IntColumnBuilder(new IntColumnFormat(COLUMN_SIZE), IntColumnNoStats)
      case "Long" => 
        new LongColumnBuilder(new LongColumnFormat(COLUMN_SIZE), LongColumnNoStats)
      case "Float" => 
        new FloatColumnBuilder(new FloatColumnFormat(COLUMN_SIZE), FloatColumnNoStats)
      case "Double" => 
        new DoubleColumnBuilder(new DoubleColumnFormat(COLUMN_SIZE), DoubleColumnNoStats)
      case "String" =>
        new TextColumnBuilder(new TextColumnFormat(COLUMN_SIZE), TextColumnNoStats)
      case _ =>
        throw new Exception("unknow primitive type: " + primitiveType)
    }
  }
  
}
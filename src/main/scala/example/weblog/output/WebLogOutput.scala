package example.weblog.output

class WebLogOutput extends TableRDDOutput {
  override def setOutputName(name: String) {
    val tblName = name + "_cached"
    super.setOutputName(tblName)
    
    name match {
      case "item_view" => setOutputDataFormat(Array("Long", "String", "Long", "Long"))
      case "subcategory_view" => setOutputDataFormat(Array("Long", "String", "String", "Long"))
      case _ => throw new Exception("unknown output name " + name)
    }
  }
}
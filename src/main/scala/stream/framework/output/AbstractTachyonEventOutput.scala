package stream.framework.output

import tachyon.client.TachyonClient
import tachyon.thrift.FileAlreadyExistException

abstract class AbstractTachyonEventOutput extends AbstractEventOutput {
  
  val masterUrl = {
    val url = System.getenv("TACHYON_MASTER")
    if (url == null)
      "localhost:19998"
    else
      url
  }
  
  val warehousePath = {
    val path = System.getenv("TACHYON_WAREHOUSE_PATH")
    if (path == null)
      "/user/tachyon"
    else
      path
  }
  
  @transient lazy val tachyonClient = TachyonClient.getClient(masterUrl) 
  
  // create data warehouse path in tachyon
  try {
    tachyonClient.mkdir(warehousePath)
  } catch {
    case e: FileAlreadyExistException => Unit
  }
  
  def getTachyonTable(colNum: Int) = {
    // create table in tachyon
    val tablePath = warehousePath + "/" + getOutputName + "_tachyon"
    val table = {
      if (tachyonClient.exist(tablePath)) {
        tachyonClient.getRawTable(tablePath)
      } else {
        val tableId = tachyonClient.createRawTable(tablePath, colNum)
        tachyonClient.getRawTable(tableId)
      }
    }
    table
  }
}
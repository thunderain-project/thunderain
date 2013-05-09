package example.weblog.operator

import shark.{SharkContext, SharkEnv}

import spark.streaming.DStream
import spark.RDD
import spark.SparkContext._

import stream.framework.Event
import stream.framework.operator.CountOperator

class JoinCountOperator extends CountOperator {
  @transient lazy val itemCateRdd = SharkEnv.sc.asInstanceOf[SharkContext]
    .sql2rdd(
      "SELECT i_item_sk, i_category FROM item_ext")
    .mapRows(r => (r.getLong(0).toLong, r.getString(1)))
    .cache
  
  override def process(stream: DStream[Event]) {
    val windowedStream = windowStream(stream, (config.window, config.slide))
    val itemStream = windowedStream.map(e => 
      (e.keyMap(config.key).toLong, e.keyMap(config.key).toLong))
      .transform(_.join(itemCateRdd))
    
    val resultStream = config.name match {
      case "item_view" => 
        itemStream.map(r => (r._2._2, r._2._1)).countByValue()
      case "subcategory_view" =>
        itemStream.flatMap(r => splitCategory(r._2._2)).countByValue()
      case _ => throw new Exception("unknown job name " + config.name)
    }
    
    outputCls.output(resultStream)
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
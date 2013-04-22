package stream.framework.util

import scala.xml._
import scala.collection.mutable.ArrayBuffer


case class Property(val name: String, val outputClass: String)

case class AggregateProperty(
    n: String,
    output: String,
    val window: (Option[Long], Option[Long]),
    val hierarchy: Option[String],
    val key: String,
    val value: String) extends Property(n, output) {
  
  override def toString = 
    "keyValue(" + key + ":" + value + ") " +
    "window(" + window._1 + ":" + window._2 + ") hierarchy(" + hierarchy + ")"
}

case class CountProperty(
    n: String,
    output: String,
    val window: (Option[Long], Option[Long]),
    val hierarchy: Option[String],
    val key: String) extends Property(n, output) {
  override def toString =  "key(" + key + ") " + 
	"window(" + window._1 + ":" + window._2 + ") hierarchy(" + hierarchy + ")"
}

case class DistinctAggregateCountProperty(
    n: String,
    output: String,
    val window: (Option[Long], Option[Long]),
    val hierarchy: Option[String],
    val key: String,
    val value: String) extends Property(n, output) {
  override def toString =  "keyValue(" + key + ":" + value + ") " + 
	"window(" + window._1 + ":" + window._2 + ") hierarchy(" + hierarchy + ")"
}


class AppConfig (
    val category: String,
    val parserClass: String,
    val outputClass: String,
    val schemas: Array[String],
    val properties: List[Property]) extends Serializable {
  override def toString = 
    "category(" + category + ") schema(" + schemas.mkString(":") + ") " +
    		"properties(" + properties.mkString(" ") + ")"
}

object Configuration {
  
  val appConfMap = 
    scala.collection.mutable.Map[String, AppConfig]()
    
  def parseConfig(xmlFilePath: String) {
    val xmlFile = XML.load(xmlFilePath)
    
    // parse each application
    xmlFile match {
      case <applications>{allApps @ _*}</applications> =>
        for (app @ <application>{_*}</application> <- allApps) {
          appConfMap += parseApp(app)
        }
    }
  }
  
  private def parseApp(appXML: Node) = {
    //parse category
    val category = (appXML \ "category").text
    
    //parse parser class
    val parser = (appXML \ "parser").text
    
    //parse output class
    val output = (appXML \ "output").text
    
    //parse items
    val array = ArrayBuffer[String]()
    ((appXML \ "items") \ "item").map(array += _.text)

    //parse property
    val properties = ((appXML \ "properties") \ "property").map(p => {
      (p \ "@type").text match {
        case "count" => {
          val name = (p \ "name").text      
          val output = (p \ "output").text
        		  		
          val window = if ((p \ "@window").length == 0) None 
        		  		else Some((p \ "@window").text.toLong)
        		  		
          val slide = if ((p \ "@slide").length == 0) None 
        		  		else Some((p \ "@slide").text.toLong)
          
          val hierarchy = if ((p \ "@hierarchy").length == 0) None 
        		  		else Some((p \ "@hierarchy").text)
        		  		
          val key = (p \ "key").text
          
          new CountProperty(name, output, (window, slide), hierarchy, key)
        }
        
        case "aggregate" => {
          val name = (p \ "name").text   
          val output = (p \ "output").text
        		  		
          val window = if ((p \ "@window").length == 0) None 
        		  		else Some((p \ "@window").text.toLong)
        		  		
          val slide = if ((p \ "@slide").length == 0) None 
        		  		else Some((p \ "@slide").text.toLong)
          
          val hierarchy = if ((p \ "@hierarchy").length == 0) None 
        		  		else Some((p \ "@hierarchy").text)
        		  		
          val key = (p \ "key").text
          val value = (p \ "value").text
          
          new AggregateProperty(name, output, (window, slide), hierarchy, key, value)
        }
        
        case "distinct_aggregate_count" => {
          val name = (p \ "name").text
          val output = (p \ "output").text
        		  		
          val window = if ((p \ "@window").length == 0) None 
        		  		else Some((p \ "@window").text.toLong)
        		  		
          val slide = if ((p \ "@slide").length == 0) None 
        		  		else Some((p \ "@slide").text.toLong)
          
          val hierarchy = if ((p \ "@hierarchy").length == 0) None 
        		  		else Some((p \ "@hierarchy").text)
        		  		
          val key = (p \ "key").text
          val value = (p \ "value").text
          
          new DistinctAggregateCountProperty(name, output, (window, slide), hierarchy, key, value)
        }
      }
    })
    
    (category, new AppConfig(category, parser, output, 
        array.toArray, properties.toList))
  }
  
}

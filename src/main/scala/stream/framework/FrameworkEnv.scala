package stream.framework

import scala.xml._
import scala.collection.mutable

import spark.streaming.DStream

import stream.framework.parser.AbstractEventParser
import stream.framework.output.AbstractEventOutput
import stream.framework.output.StdEventOutput
import stream.framework.operator._

object FrameworkEnv {
  //hash map for all operators in one framework
  val operators = new mutable.HashMap[String, String]
  
  //hash map for all applications in one framework
  val apps = new mutable.HashMap[String, AppEnv]
  
  def parseConfig(xmlFilePath: String) {
    val xmlFile = XML.load(xmlFilePath)
    
    (xmlFile \ "operators" \ "operator").foreach(n => {
      operators.put((n \ "@type").text, (n \ "@class").text)
    })
    
    // parse each application
    xmlFile match {
      case <applications>{allApps @ _*}</applications> =>
        for (appConf @ <application>{_*}</application> <- allApps) {
          val app = new AppEnv(appConf)
          apps += ((app.category, app))
          
        }
    } 
  }
  
  def setHierarchyFunc(app: String, job: String, f: String => Array[String]) {
    apps(app).jobfuncs += ((job, f))
  }
  
  class AppEnv(conf: Node) extends Serializable {
    var category: String = _
    private var parser: AbstractEventParser = _
    private var items: Array[String] = _
    
    val jobs = new mutable.HashMap[String, AbstractOperator]
    val jobfuncs = new mutable.HashMap[String, String => Array[String]]
    
    parseAppConf(conf)
    
    def parseAppConf(conf: Node) {
      category = (conf \ "category").text
      
      val parserCls = (conf \ "parser").text 
      parser = try {
        Class.forName(parserCls).newInstance().asInstanceOf[AbstractEventParser]
      } catch {
        case e: Exception => println(e.getStackTraceString); exit(-1)
      }
      
      items = (conf \ "items" \ "item").map(_.text).toArray
      
      (conf \ "jobs" \ "job").foreach(j => {
        val name = (j \ "@name").text
        val typ = (j \ "@type").text
        
        val op = operators.get(typ) match {
          case Some(s) => Class.forName(s).newInstance().asInstanceOf[OperatorConfig]
          case None => throw new Exception("type " + typ + " cannot find related operator")
        }
        op.parseConfig(j)
        
        jobs.put(name, op.asInstanceOf[AbstractOperator]) 
      })
    }
    
    def process(stream: DStream[String]) {
      val eventStream = stream.map(s => parser.parseEvent(s, items))
      
      jobs.foreach(j => j._2.process(eventStream, jobfuncs.getOrElse(j._1, null)))
    }
  }
}
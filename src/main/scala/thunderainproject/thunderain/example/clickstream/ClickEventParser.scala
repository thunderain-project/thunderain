package thunderainproject.thunderain.example.clickstream

import thunderainproject.thunderain.framework.parser.AbstractEventParser
import thunderainproject.thunderain.framework.Event

class ClickEventParser extends AbstractEventParser {

  override def parseEvent(event: String, schema: Array[String]) = {
    val parts = event.split("\\|")
    assert(parts.length == schema.length,
        "parsed result number is not equal to property number")
        
    new Event(System.currentTimeMillis() / 1000, schema.zip(parts).toMap)
  }  
}
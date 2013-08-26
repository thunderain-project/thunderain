package thunderainproject.thunderain.example.qiyi_log

import thunderainproject.thunderain.framework.parser.AbstractEventParser
import thunderainproject.thunderain.framework.Event

class QiyiLogParser extends AbstractEventParser {

  override def parseEvent(event: String, schema: Array[String]) = {
    val beg = event.indexOf("timelen=")
    val tmlen = if (beg != -1) {
      val end = event.indexOf("&", beg)
      if (end != -1) {
        event.substring(beg + 8, end)
      } else {
        "0"
      }
    } else {
      "0"
    }

    assert(schema.size == 2, "schema number should be 2")

    new Event(System.currentTimeMillis() / 1000, schema.zip(Array("1", tmlen)).toMap)
  }

}

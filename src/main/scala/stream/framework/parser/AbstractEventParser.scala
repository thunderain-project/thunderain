package stream.framework.parser

import stream.framework.Event

abstract class AbstractEventParser {
  
  /**
   * parse the input event string, should be implemented by derivative
   * @param event: input event string
   * @return Event
   */
  def parseEvent(event: String, schema: Array[String]): Event
}
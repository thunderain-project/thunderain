package thunderainproject.thunderain.framework

class Event(val timestamp: Long,
			val keyMap: Map[String, String]) extends Serializable {
  override def toString() = {
    "Event: timestamp[" +  timestamp + "] " + keyMap.mkString("[", "][", "]")
  }
}
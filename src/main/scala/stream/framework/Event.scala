package stream.framework

class Event(val timestamp: Long,
			val keyMap: Map[String, String]) extends Serializable
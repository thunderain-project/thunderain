package stream.framework.output

import spark.streaming.DStream

abstract class AbstractEventOutput extends Serializable{
  private var outputName: String = ""

  /**
   * abstract method of output DStream, derivatives should implement this.
   */
  def output(stream: DStream[_])
  
  def setOutputName(name: String) {
    outputName = name
  }
  
  def getOutputName = outputName
}

class StdEventOutput extends AbstractEventOutput {
  
  override def output(stream: DStream[_]) {
    stream.print()
  }
}
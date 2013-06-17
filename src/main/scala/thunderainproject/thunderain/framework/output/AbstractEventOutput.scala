package thunderainproject.thunderain.framework.output

import spark.streaming.DStream

abstract class AbstractEventOutput extends Serializable{
  protected var formats: Array[String] = _
  protected var outputName: String = _
  
  /**
   * abstract method of output DStream, derivatives should implement this.
   */
  def output(stream: DStream[_])
  
  /**
   * set the output name, derivatives can use it to set output name.
   */
  def setOutputName(name: String) {
    outputName = name
  }
  
  /**
   * set the output data format, derivatives can use it to set formats.
   */
  def setOutputDataFormat(formats: Array[String]) {
    this.formats = formats
  }
  
  /**
   * preprocess the output stream, derivatives can override this.
   */
  def preprocessOutput(stream: DStream[_]): DStream[_] = {
    stream
  }
}

class StdEventOutput extends AbstractEventOutput {
  override def output(stream: DStream[_]) {
    stream.print()
  }
}
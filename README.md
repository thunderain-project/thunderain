Thunderain
==============

**Thunderain** is a Real-Time Analytical Processing (**RTAP**) example using [Spark](http://spark-project.org/) and [Shark](http://shark.cs.berkeley.edu/), which can be best characterized by the following four salient properties:

 * Data continuously streamed in & processed in near real-time
 * Real-time data queried and presented in an online fashion
 * Real-time and history data combined and mined interactively
 * Predominantly RAM-based processing

For more details, please refer to our presentation at the AMPLab retreat in May 2013.

---

The Thunderain example provide a basic RTAP framework that

 * Allows multiple application (App) to be defined, each of which is bound to a [Kafka](http://kafka.apache.org/) topic
 * Fetches data streamed in from the kafka message queue
 * Parses the data stream and then processes the parsed data for counting & aggregation (similar to [RainBird](http://www.slideshare.net/kevinweil/rainbird-realtime-analytics-at-twitter-strata-2011)) using Spark Streaming
 * Outputs the processed results to a cached table, which can then be queried through Shark

To define an App, the user need to specify

 * The parser (implementing `AbstractEventParser`) to parse the data stream; several parsers (e.g., `ClickEventParser` and `WebLogParser`) are provided in the example
 * One or more jobs, each of which
   * Performs an operation (implementing both `AbstractOperator` and `OperatorConfig`) on the streaming data; several operators (e.g., `CountOperator`, `AggregateOperator` and `DistinctAggregateCountOperator`) are provided in the example
   * Writes the processed results using an outputer (implementing ` AbstractEventOutput `); several outputers (e.g., ` StdEventOutput `, ` TableRDDOutput` and ` TachyonRDDOutput `) are provided in the example

For more details, please refer to [wiki](https://github.com/thunderain-project/thunderain/wiki).

---

The Thunderain example provides two RTAP applications (i.e., **clickstream** and **weblog**), as defined in `conf/properties.xml`. They have been tested at our internal Spark/Shark deployment (which are available at <https://github.com/thunderain-project/spark> and <https://github.com/thunderain-project/shark>). To run the applications, one need to

 * Build the project by `sbt package`
 * Configure related properties (e.g., log4j, Spark fairScheduler, etc.) in the `conf/` directory
 * Launch the framework by `run thunderainproject.thunderain.framework.Thunderain <config file list>`

 ---

 Thunderain is open source under a Apache License Version 2.0.

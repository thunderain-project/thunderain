streaming-demo
==============

A Spark Streaming demo framework is a framework for streaming data counting and aggregating, like Twitter Rainbird, but has more feature compared to it.

* it has several kinds of operators, not only counter operator (like in Rainbird). Also user can implement self-defined operators for your own needs.
* it can process different topics of Kafka message with topic specific application in one framework.
* it is highly configurable with XML file.
* it embeds SharkServer in its framwork, users can output streaming result to Shark's TableRDD and connect to SharkServer for real-time query.

---

The whole architecture of streaming demo is like this:

<img src="https://dl.dropboxusercontent.com/u/19230832/streaming_cluster_architecture.png" alt="cluster architecture" width="480"/>

Here data is collected from web server and transfered by Kafka message queue, Spark Streaming cluster will fetch data from Kafka in each batch duration and process it. Processed data can be put into in memory table using Shark's readable format. User can connect to embeded SharkServer for querying, Also user can self-implement output class to store processed data in any other way.

The UML class chart is:

<img src="https://dl.dropboxusercontent.com/u/19230832/streaming_uml.png" alt="streaming uml" width="480"/>

Here each App is bound to Kafka's each topic, if you want to process several kinds of topics in one framework, you should implement each topic related App.

In each App, user can:

* Inherit from `AbstractEventParser` to implement a self-defined parser to class to parse input streaming data.
* Inherit from `AbstractEventOutput` to implement a self-defined output class to store processed data.
* Use 3 operators (`CountOperator`, `AggregateOperator`, `DistinctAggregateCountOperator`) I've already implemented or make your own operator by implementing `AbstractOperator` and `OperatorConfig`.

---

User want to use this framework should configure the XML file `conf/properties.xml`, take below examples

    <applications>
        <operators>
            <operator type="count" class="stream.framework.operator.CountOperator"/>
            <operator type="distinct_aggregate_count" class="stream.framework.operator.DistinctAggregateCountOperator"/>
            <operator type="aggregate" class="stream.framework.operator.AggregateOperator"/>
            <operator type="join_count" class="example.weblog.operator.JoinCountOperator"/>
        </operators>
        <application>
            <category>clickstream</category>
            <parser>example.clickstream.ClickEventParser</parser>
            <items>
                <item>source_ip</item>
                <item>dest_url</item>
                <item>visit_date</item>
                <item>ad_revenue</item>
                <item>user_agent</item>
                <item>c_code</item>
                <item>l_code</item>
                <item>s_keyword</item>
                <item>avg_time_onsite</item>
            </items>
            <jobs>
                <job name="page_view" type="count">
                    <property window="30" slide="10"/>
                    <key>dest_url</key>
                    <output>stream.framework.output.StdEventOutput</output>
                </job>
                <job name="user_visit_count" type="distinct_aggregate_count">
                    <property window="30" slide="10"/>
                    <key>dest_url</key>
                    <value>source_ip</value>
                    <output>stream.framework.output.StdEventOutput</output>
                </job>
                <job name="user_visit" type="aggregate">
                    <property window="30" slide="10"/>
                    <key>dest_url</key>
                    <value>source_ip</value>
                    <output>stream.framework.output.StdEventOutput</output>
                </job>
            </jobs>
        </application>
        <application>
            <category>weblog</category>
            <parser>example.weblog.WebLogParser</parser>
            <items>
                <item>source_ip</item>
                <item>cookie_id</item>
                <item>visit_date</item>
                <item>item_id</item>
                <item>referrer</item>
                <item>agent</item>
            </items>
            <jobs>
                <job name="item_view" type="join_count">
                    <property window="30"/>
                    <key>item_id</key>
                    <output>example.weblog.output.WebLogTachyonOutput</output>
                </job>
                <job name="subcategory_view" type="join_count">
                    <property window="30"/>
                    <key>item_id</key>
                    <output>stream.framework.output.StdEventOutput</output>
                </job>
            </jobs>
        </application>
    </applications>

First all the operators you used in your Apps should list in `<operators></operators>`. In each operator, `type` is the name of the operator, which should be the same in each job property `type` of each App; `class` is the class of the operator.

Here several `application` can co-exists in one `applications`, I've already configure two applications "clickstream" and "weblog" in one configuration. User can configure `application` specific parameter like above:

1. `category` is the category of Kafka messge, also use this as the topic name in Kafka.
2. `parser` is the user defined parser class class, user should extends `AbstractParser` to self-defined one.
3. `items` is the schema of input message, in case input message has several items, this is the name of each item.
4. `jobs` is the configuration of jobs in each application, you should specify a job `name` and `type`, `type` is the same as operator's `type` which means how job deals with streaming data, count or aggregate as example.

    configuration in each `job` is operator specified, if you implement a new operator like "join_count", you should take responsiblity to parse configurations like `property`, `key` etc...

        <job name="item_view" type="join_count">
            <property window="30"/>
            <key>item_id</key>
            <output>example.weblog.output.WebLogTachyonOutput</output>
        </job>

Currently framework supports 3 operators for user:

* `CountOperator`: `CountOperator` will count the occurrence of specific `key`, like PV (page views), here are several parameters related to `CountOperator`,
    * `window`: specify the timing window for Spark Streaming to collect data and calculate
    * `slide`: specify the sliding parameter for this window, take `10` as example, Spark Streaming will process data in each 10 seconds for 30 seconds window data.
    * `output`: specify the output class you implemented to store the output data. `output` should extend `AbstractEventOutput`.
* `AggregateOperator`: `AggregateOperator` will aggregate the `value` by `key` which you specified, and the parameters of this operator is the same as `CountOperator`.
* `DistinctAggregateCountOperator`: `DistinctAggregateCountOperator` will count the distinct `value` with specified `key`, also parameters is the same as above.
* besides, user can create their own `Operator` by extending `AbstractOperator`, which is easy and obvious.

---

By running this demo, a log4j configuration is required, also shark-env.sh should be configured. If you want to enable fair scheduler in this demo, a fair scheduler configuration is required. to run this demo, by typing:

    ./run stream.framework.StreamingDemo conf/properties.xml conf/log4j.properties <conf/fairscheduler.xml>

Ps.

1. Currently Spark Streaming's Kafka receiver only support string codec.
2. to differentiate topics received from Kafka, current workaround method is to add topic to each incoming record, like:

        clickstream + "|||" + record

   Because Spark Streaming will receive all the topics record in one `DStream` without differentiating which data to which topic, so a self-defined filter is needed.

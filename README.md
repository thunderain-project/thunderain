streaming-demo
==============

A Spark Streaming demo framework is a framework for streaming data counting and aggregating, like Twitter Rainbird, but has more feature compared to it.

* it has several kinds of operators, not only counter operator (like in Rainbird).
* it can process different topics of Kafka message with topic specific parser in one framework.
* it is highly configurable with XML file.


The whole architecture of streaming demo is like this:

![architecture of streaming demo](http://dl.dropbox.com/u/19230832/streaming_cluster_architecture.png "architecture")

The UML class chart is:

![UML class chart](http://dl.dropbox.com/u/19230832/streaming_uml.jpg "uml")

Users want to use this framework should configure the XML file `conf/properties.xml`, like `clickstream` examples

    <applications>
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
            <properties>
                <property window="30" slide="10" hierarchy="/" type="count">
                    <key>dest_url</key>
                    <output>stream.framework.output.TachyonStrToLongOutput</output>
                </property>
                <property window="30" slide="10" hierarchy="/" type="aggregate">
                    <key>dest_url</key>
                    <value>source_ip</value>
                    <output>stream.framework.output.TachyonStrToStrOutput</output>
                </property>
                <property window="30" slide="10" hierarchy="/" type="distinct_aggregate_count">
                    <key>dest_url</key>
                    <value>source_ip</value>
                    <output>stream.framework.output.TachyonStrToLongOutput</output>
                </property>
            </properties>
        </application>
    </applications>

Here several `application` can co-exists in one `applications`, user can configure `application` specific parameter like above:

1. `category` is the category of Kafka messge, also use this as the topic.
2. `parser` is the user defined parser class class, user should extends `AbstractParser` to self-defined ones.
3. `items` is the schema of input message, in case input message has several items, this is the name of each item.
4. `properties` is the one you want to operate, you could specify several kinds of operators with some properties

Currently framework supports 3 operators for user:

* `CountOperator`: `CountOperator` will count the occurrence of specific `key`, like PV (page views), here are several parameters related to `CountOperator`,
    * `window`: specify the timing window for Spark Streaming to collect data and calculate
    * `slide`: specify the sliding parameter for this window, take `10` as example, Spark Streaming will process data in each 10 seconds for 30 seconds window data.
    * `hierarchy`: `hierarchy` means if you want to delimit data using specified delimiter, like `/`. As for page view analysis, each url should split like this:

            http://xyz.com/aaa/bbb/ccc => xyz.com/aaa/bbb/ccc xyz.com/aaa/bbb xyz.com/aaa xyz.com
    * `type`: specify the operator you choose.
    * `output`: specify the output class you implemented to store the output data in this kind of data. `output` should extend `AbstractEventOutput`.
* `AggregateOperator`: `AggregateOperator` will aggregate the `value` by `key` which you specified, and the parameters of this operator is the same as `CountOperator`.
* `DistinctAggregateCountOperator`: `DistinctAggregateCountOperator` will count the distinct `value` with specified `key`, also parameters is the same as above.
* besides, user can create their own `Operator` by extends `AbstractOperator`, which is easy and obvious.


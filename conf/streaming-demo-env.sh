#!/bin/sh

# spark configuration
export SPARK_MASTER_URL=local[2]
export SPARK_HOME=/home/jerryshao/source-code/spark-0.7.0/
export STREAM_JAR_PATH=/home/jerryshao/source-code/streaming-demo/target/scala-2.9.2/streaming-demo_2.9.2-0.0.1.jar

# kafka configuration
export ZK_QUORUM=localhost:2181
export KAFKA_GROUP=test

# tachyon configuration
export TACHYON_MASTER=localhost:19998
export TACHYON_WAREHOUSE_PATH=/user/tachyon

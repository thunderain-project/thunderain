#!/bin/sh

# spark configuration
export SPARK_MASTER_URL=local[10]
export SPARK_HOME=/home/jerryshao/source-code/spark-0.7.0/
export STREAM_JAR_PATH=/home/jerryshao/source-code/streaming-demo/target/scala-2.9.2/streaming-demo_2.9.2-0.0.1.jar
export DATA_CLEAN_TTL=600
# kafka configuration
export ZK_QUORUM=localhost:2181
export KAFKA_GROUP=test
export KAFKA_INPUT_NUM=4

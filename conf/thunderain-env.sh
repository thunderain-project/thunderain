#!/bin/sh
export DATA_CLEAN_TTL=3600
# disable the shark service means not able to access those cached tables in shark service's memory space
# all those shark memory table outputs are invalid
export DISABLE_SHARK_SERVICE=

# kafka configuration
export ZK_QUORUM=10.0.2.12:2181
export KAFKA_GROUP=test
export KAFKA_INPUT_NUM=6

# tachyon configuration
export TACHYON_MASTER=10.0.2.12:19998
export TACHYON_WAREHOUSE_PATH=/user/tachyon

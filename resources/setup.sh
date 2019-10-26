#!/usr/bin/env bash
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic demo
./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic demo


# https://10.5.253.200
#
# root/Zhangshize@123
#
# &xcCCe$7yx42gedE
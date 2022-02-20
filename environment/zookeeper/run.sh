#! /usr/bin/env sh

# 创建data
mkdir /opt/zookeeper/zkData
echo $MY_ID > /opt/zookeeper/zkData/myid

zkServer.sh start-foreground

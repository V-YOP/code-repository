#! /usr/bin/env sh

# 创建data
mkdir /opt/zookeeper/zkData
echo $MY_ID > /opt/zookeeper/zkData/myid

# start-foreground命令会让服务器在前台执行，因此适合作为镜像的ENTRYPOINT
zkServer.sh start-foreground

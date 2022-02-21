FROM fedora

# ssh，以及一些方便debug的玩意...
RUN yum install -y openssh-server openssh-clients hostname util-linux procps vim findutils
# 这hadoop依赖好多终端命令……就连调个example都能报出来个什么command not found，宁开玩笑吗？

# ./source/runtime_environment/hadoop/, ./source/runtime_environment/node/, ./source/runtime_environment/jdk8/
# ADD命令会解压压缩文件！
# 解压各tar.gz并修改文件夹名称！如果更换了版本，记得修改文件夹名称
ADD ./*.tar.gz /opt/
RUN mv /opt/apache-zookeeper-3.5.9-bin/ /opt/zookeeper/    &&\
    mv /opt/jdk8u322-b06/               /opt/jdk8/

ENV PATH="/opt/zookeeper/bin:/opt/jdk8/bin:${PATH}" \
    JAVA_HOME="/opt/jdk8" \
    ZOOKEEPER_HOME="/opt/zookeeper"

COPY ./zoo.cfg ${ZOOKEEPER_HOME}/conf/
COPY ./run.sh ${ZOOKEEPER_HOME}/
 
ENTRYPOINT ${ZOOKEEPER_HOME}/run.sh
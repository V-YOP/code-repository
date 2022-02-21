Zookeeper集群，练习环境搭建用，需在根目录添加jdk8和zookeeper的二进制包。

- [jdk8](https://mirrors.tuna.tsinghua.edu.cn/AdoptOpenJDK/8/jdk/x64/linux/OpenJDK8U-jdk_x64_linux_hotspot_8u322b06.tar.gz)
- [zookeeper](https://mirrors.tuna.tsinghua.edu.cn/apache/zookeeper/zookeeper-3.5.9/apache-zookeeper-3.5.9-bin.tar.gz)

## 环境搭建步骤

1. 安装依赖，设置环境变量
2. 根据sample文件创建`zoo.cfg`，配置`dataDir`和`server.x`
3. 创建`dataDir`指向的文件夹，创建myid文件，其保存本节点的id
4. 在每个节点执行`zkServer.sh start`
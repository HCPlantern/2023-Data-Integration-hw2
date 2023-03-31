# 2023-数据集成-路线二第二次作业报告

[toc]

## 1. 基本信息

**小组编号：4**

| 姓名   | 学号      | 分工 |
| ------ | --------- | ---- |
| 万沛沛 | 201250038 | Flink 流数据处理部分     |
| 邓尤亮 | 201250035 | 主机环境配置、可视化部分、获取助教 Kfaka 数据     |
| 韩陈旭 | 201250037 | Flink 流数据处理部分     |
| 张月明 | 201830115 | 数据库表批处理部分、Kafka 生产者     |
| 华广松 | 201840309 | 数据库表批处理部分、Kafka 生产者     |

## 2. 项目文件

项目仓库：[https://github.com/HCPlantern/2023-Data-Integration](https://github.com/HCPlantern/2023-Data-Integration)

## 3. 环境搭建

### 3.1 基本信息

该部分介绍本次项目的运行主机信息、团队协作方式。

#### 3.1.1 主机信息

我们使用了一个搭载 J4125 的工控机平台作为本次实验的主机，使用 PVE 虚拟平台创建了一个 Ubuntu 20.04 虚拟机，并分配内存 8G、硬盘 120 GB。该主机运行在南大宿舍，并借助路由器向南大校园网暴露部分端口以便于团队成员访问和开发。

![](https://i.imgur.com/NhxeTLS.png)


#### 3.1.2 团队协作

本次实验的不同模块代码全部上传至 Github，并在协作方面采用 Github Workflow 即 Pull Request 的方式进行代码合作开发和 Code Review，有关开发过程请见 [PR 记录](https://github.com/HCPlantern/2023-Data-Integration/pulls?q=is%3Apr+is%3Aclosed)。


### 3.2 环境说明

#### 3.2.1 版本展示

以下展示主机所搭建的所有相关组件信息，其中带 “\*” 的表示运行在 docker 容器中。

| 组件         | 版本                         | 备注                    |
| ------------ |:---------------------------- | ----------------------- |
| 宝塔面板     | 7.9.8                        | 访问文件、文件管理      |
| JDK          | jdk-8u361                    |                         |
| Hadoop       | 2.7.4                        |                         |
| Spark        | 2.3.3                        |                         |
| Flink*        | flink:1.13.5-scala_2.11-java8|                         |
| Zookeeper*   | bitnami/zookeeper:3.4.10     |                         |
| Kafka*       | bitnami/kafka:2.1.0          |                         |
| Kafka Eagle* | nickzurich/kafka-eagle:2.1.0 | 用于监视 Kafka 并展示信息，内存占用较大 |
| Clickhouse*  | clickhouse-server:latest     |                         |
| Dataease*    |registry.cn-qingdao.aliyuncs.com/dataease/dataease:v1.18.5| 可视化平台    |



### 3.3 搭建过程

#### 3.3.1 宝塔面板

运行官网脚本即可。

#### 3.3.2 JDK Hadoop Spark

完全按照以下参考资料进行安装：
1. [hadoop 2.7.4 单机版安装](https://blog.csdn.net/isoleo/article/details/78394777)
2. [spark2.4的安装和基本使用](https://blog.csdn.net/walkcode/article/details/104208855)

#### 3.3.3 Flink

我们选择了 Docker Hub 中 [flink:1.13.5-scala_2.11-java8](https://hub.docker.com/_/flink) 这个镜像，具体部署参考了：
- http://web.archive.org/web/20220828125423/https://www.awaimai.com/2934.html

docker-compose.yaml 文件如下：

```yaml
version: '3'

services:
  jobmanager:
    container_name: jobmanager
    image: flink:1.13.5-scala_2.11-java8
    restart: always
    ports:
      - 8090:8081
    command: jobmanager
    environment:
      - TZ=Asia/Shanghai
      - JOB_MANAGER_RPC_ADDRESS=jobmanager
      - |
        FLINK_PROPERTIES=
        taskmanager.numberOfTaskSlots: 4
        jobmanager.memory.process.size: 2600m
        taskmanager.memory.process.size: 2728m
        taskmanager.memory.flink.size: 2280m
    volumes:
      - ./data:/opt/flink/data
    networks:
      - clickhouse_default
      - kafka_default

  taskmanager:
    container_name: taskmanager
    image: flink:1.13.5-scala_2.11-java8
    restart: always
    expose:
      - "6121"
      - "6122"
    depends_on:
      - jobmanager
    command: taskmanager
    links:
      - "jobmanager:jobmanager"
    environment:
      - TZ=Asia/Shanghai
      - JOB_MANAGER_RPC_ADDRESS=jobmanager
      - |
        FLINK_PROPERTIES=
        taskmanager.numberOfTaskSlots: 4
        jobmanager.memory.process.size: 2600m
        taskmanager.memory.process.size: 2728m
        taskmanager.memory.flink.size: 2280m
    volumes:
      - ./data:/opt/flink/data
    networks:
      - clickhouse_default
      - kafka_default

networks:
  clickhouse_default:
    external: true
  kafka_default:
    external: true

```

该部分依赖 Kafka 以及 Clickhouse 的网络。基于该镜像我们分别部署了 jobmanager 以及 taskmanager 这两个 docker 服务。

#### 3.3.4 Zookeeper & Kafka & Kafka Eagle

该部分我们参考了：
- [Kafka(Go)教程(一)---通过docker-compose 安装 Kafka](https://www.lixueduan.com/posts/kafka/01-install/)

docker-compose.yaml 文件如下：

```yaml
version: "3"
services:
  zookeeper:
    image: 'bitnami/zookeeper:3.5'
    user: root
    container_name: zookeeper
    restart: always
    ports:
      - '2181:2181'
    environment:
      # 匿名登录--必须开启
      - ALLOW_ANONYMOUS_LOGIN=yes
    volumes:
      - ./zookeeper:/bitnami/zookeeper
  kafka:
    image: 'bitnami/kafka:2.1.0'
    user: root
    container_name: kafka
    restart: always
    ports:
      - '9092:9092'
      - '9999:9999'
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_LISTENERS=PLAINTEXT://:9092
      # 客户端访问地址，更换成自己的
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092
      - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
      # 允许使用PLAINTEXT协议(镜像中默认为关闭,需要手动开启)
      - ALLOW_PLAINTEXT_LISTENER=yes
      # 关闭自动创建 topic 功能
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=false
      # 全局消息过期时间 6 小时(测试时可以设置短一点)
      - KAFKA_LOG_RETENTION_HOURS=6
      # 开启JMX监控
      - JMX_PORT=9999
    volumes:
      - ./kafka:/bitnami/kafka
    depends_on:
      - zookeeper
  kafka-eagle:
      image: nickzurich/kafka-eagle:2.1.0
      container_name: kafka-eagle
      restart: unless-stopped
      environment:
        - TZ=Asia/Shanghai
        - EFAK_CLUSTER_ZK_LIST=zookeeper:2181
        - EFAK_DB_DRIVER=com.mysql.jdbc.Driver
        - EFAK_DB_USERNAME=root
        - EFAK_DB_PASSWORD=123456
        - EFAK_DB_URL=jdbc:mysql://mysql:3306/ke?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull
      depends_on:
        - kafka
        - mysql
      ports:
        - 8048:8048
  mysql:
    image: mysql
    restart: always
    environment:
      - MYSQL_ROOT_PASSWORD=123456

```

该部分部署了四个服务，其中 Zookeer 和 Kafka 使用了助教推荐的版本，镜像由 bitnami 提供；Kafka Ealge 用于监控 Kafka 实时数据，MySql 是 Kafka Eagle 的数据库服务。

在部署时，我们遇到了一些坑：
1. Kafka 选择 2.1.0 镜像，按照教程启动后无法连接 zookeeper，经过 docker inspect 之后发现是 zookeeper 连接地址的环境变量的字段在新版本更新过（KAFKA_ZOOKEEPER_CONNECT -> KAFKA_CFG_ZOOKEEPER_CONNECT）。修改为旧字段之后成功连接。
2. Bitnami 提供的容器默认使用 Non-Root Containers，导致添加 volumes 时容器内因无权限创建文件夹而报错 Permission Denied。解决办法是在 docker-compose.yaml 中添加一行配置 user:root

#### 3.3.5 Clickhouse

我们选择的镜像为 [yandex/clickhouse-server:latest](https://hub.docker.com/r/yandex/clickhouse-server)，参考了如下教程：
- [ClickHouse 简单部署&使用测试](https://zhuanlan.zhihu.com/p/383817560)

docker-compose.yaml 文件如下

```yaml
version: '3'

services:
  clickhouse:
    container_name: clickhouse
    image: yandex/clickhouse-server:latest 
    restart: always
    ports:
      - '8123:8123' # HTTP
      - '9001:9000' # TCP
      - '9009:9009' # Inter-server HTTP port
    volumes: 
      # 默认配置 写入config.d/users.d 目录防止更新后文件丢失
      - ./config.xml:/etc/clickhouse-server/config.d/config.xml:rw
      - ./users.xml:/etc/clickhouse-server/users.xml:rw
      # 运行日志
      - ./logs:/var/log/clickhouse-server
      # 数据持久
      - ./data:/var/lib/clickhouse:rw
```

由于端口冲突，这里将 TCP 端口映射为了 9001。

#### 3.3.6 Dataease

该部分参考官网部署文档，直接运行脚本即可：

```bash!
curl -sSL https://dataease.oss-cn-hangzhou.aliyuncs.com/quick_start.sh | sh
```

为了使该可视化平台能够在公网运行，我们借助了一台公网服务器进行反向代理。具体制作的可视化作品请参照本文档的可视化部分。

#### 3.3.7 总结

docker 服务一览：

![](https://i.imgur.com/62wALsN.png)

JPS 一览：

![](https://i.imgur.com/67eSpUd.png)



## 4. 数据库表部分 @hgs @zym

### 4.1 代码流程说明

### 4.2 效果展示

任务提交至 Spark 后界面展示如图：

![](https://i.imgur.com/SZVR9Z1.png)

运行一次我们的 Spark 任务总用时约为 34min。


## 5. 流式数据部分 @wpp @hcx

### 5.1 代码流程说明

### 5.2 效果展示

我们根据 Flink 的处理能力控制了消息的推送速度。在一次 Flink Job 中，Kafka 消息的推送速率如下：

![](https://i.imgur.com/w2imFkq.png)

开始部分推送速率维持在 6k/s 左右，后续可能由于主机散热受限，推送速率下降至 4k/s 左右。总推送平均速率为 4.7k /s 左右。

Flink Job 消费端部分的消费速度与推送速度接近。根据我们的观察，在刚开始时消息挤压的总消息量为 60w 条，由于后续生产者推送速度的下降，挤压量并未明显上升，全程维持在 100w 以下的水平。

运行 50 分钟之后的情况截图：
![](https://i.imgur.com/xzZgYkE.png)

![](https://i.imgur.com/U7kZotr.png)

最终，推送消息用时 90分钟，Flink Job 总用时 93 分钟左右。

![](https://i.imgur.com/PYumrYu.png)



## 6. 可视化部分

本部分我们借助了开源数据可视化分析工具 [Dataease](https://dataease.io/) 完成该部分任务。该平台部署在主机上，并通过公网服务器反向代理到公网，以便于公网访问。

在线访问地址：
1. 各类交易数据: https://dataease.hcplantern.cn/link/hfFAUWJQ
2. 银行账户数据: https://dataease.hcplantern.cn/link/xJo6XXzf

### 6.1 各类交易数据

该场景中我们按照日期展示了各消费类型的消费总额，以及在某一时间内手机银行交易数额 TOP 20 的用户。

![](https://i.imgur.com/RUjpeie.png)

### 6.2 银行账户信息

该场景中我们展示了所有银行帐户中定期余额和贷款余额的占比，并按照日期展示贷款还本和贷款还息的总额，最后统计了合同的分类等级情况，以便于银行根据当日数据规划资金安排。

![](https://i.imgur.com/BblvomF.png)






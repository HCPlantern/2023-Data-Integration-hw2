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

```bash
curl -sSL https://dataease.oss-cn-hangzhou.aliyuncs.com/quick_start.sh | sh
```

为了使该可视化平台能够在公网运行，我们借助了一台公网服务器进行反向代理。具体制作的可视化作品请参照本文档的可视化部分。

#### 3.3.7 总结

docker 服务一览：

![](https://i.imgur.com/62wALsN.png)

JPS 一览：

![](https://i.imgur.com/67eSpUd.png)



## 4. 数据库表部分

### 4.1 代码流程说明

>所有的代码都放在`spark`项目目录下

整个数据库表部分处理分为以下步骤：

1. spark配置及远程数据库连接
2. 处理elt小作业–pri_cust_contact_info
3. 处理剩余的表
4. 数据存入ClickHouse
#### 4.1.1 spark配置及远程数据库连接

##### 4.1.1.1 部署spark

```scala
//部署spark并设置`spark://hcplantern-ubuntu:7077`为master节点
val conf = new SparkConf()
        .setAppName(this.getClass.getSimpleName)
        .setMaster("spark://hcplantern-ubuntu:7077")

//sparksession 读取数据入口
val session = SparkSession.builder()
        .config(conf)
        .getOrCreate()

```

##### 4.1.1.2 利用官方hive驱动连接远程hive静态仓库

```scala
//利用官方hive驱动连接远程hive静态仓库，DataFrameReader
val reader = session.read.format("jdbc")
        .option("url","jdbc:hive2://172.29.4.17:10000/default")
        .option("user", "student")
        .option("password", "nju2023")
        .option("driver","org.apache.hive.jdbc.HiveDriver")

val registerHiveDqlDialect = new RegisterHiveSqlDialect()

registerHiveDqlDialect.register()
```

#### 4.1.2 elt说明

##### 4.1.2.1 elt小作业--pri_cust_contact_info

首先去掉列名中的表名前缀，方便解析

```scala
val columnNames = df.columns.toList.map(name => name.substring(tblNameDst.length + 1)).toArray
df = df.toDF(columnNames: _*)
```

然后过滤contact中的无效行，去掉不需要显示的列，丢弃含有null或者NAN的行，对于联系方式根据uid去重，最后添加新列`contact_phone`和`contact_address`

```scala
df = df.where("contact != '⽆' and contact != '-' and contact != ''")//过滤无效行
df = df.drop("sys_source", "create_date", "update_date") //去掉不需要显示的列
df = df.na.drop(List("contact")) //丢弃含有null的行
df = df.dropDuplicates("uid", "contact") //根据uid去重联系方式
df = df.withColumn("contact_phone", col("con_type") + 1) //添加新列
df = df.withColumn("contact_address", col("con_type") + 1) //添加新列
```

接着将dataFrame转换成rdd进行操作，rdd.map()对每行数据进行映射得到rdd[k,v]格式：（uid,ListBuffer)。其中ListBuffer: 0:`con_type` 1:`contact` 2:`contact_phone` 3:`contact_address` ,使用逗号分隔,之后对`con_type`判断来填充ListBuffer。

```scala=
var res = df.rdd.map(row => row.getAs("uid").toString -> ListBuffer[String](row.getAs("con_type").toString, row.getAs("contact").toString, "", ""))
//对con_type判断来填充ListBuffer
res = res.map(item => {
    val listBufferValue = item._2
    if (listBufferValue.head == "TEL" || listBufferValue.head == "OTH" || listBufferValue.head == "MOB") {
        listBufferValue(2) = listBufferValue(1)
    } else {
        listBufferValue(3) = listBufferValue(1)
    }
    item
})
```

最后将rdd两两聚合，r1、r2为key相同的两个value,将r2的信息合并到r1上
```scala=
res = res.reduceByKey((r1, r2) => {
    if (r2.head == "TEL" || r2.head == "OTH" || r2.head == "MOB") {
        if (r1(2).nonEmpty) {
            r1(2) = r1(2) + "," + r2(1)
        } else {
            r1(2) = r2(1)
        }
    } else { //r2的类型为address
        if (r1(3).nonEmpty) {
            r1(3) = r1(3) + "," + r2(1)
        } else {
            r1(3) = r2(1)
        }
    }
    r1
})
val structFields = Array(StructField("uid", StringType, true), StructField("contact_phone", StringType, false), StructField("contact_address", StringType, false))
val structType = StructType(structFields)
val rdd = res.map(item => Row(item._1, item._2(2), item._2(3)))
df = session.createDataFrame(rdd, structType)
```

#### 4.1.2.2 其余表的etl说明
对于剩余的表，我们首先去掉列名中的表名前缀

```scala
val columnNames = df.columns.toList.map(name => name.substring(tblNameDst.length + 1)).toArray
df = df.toDF(columnNames: _*)
```

然后删掉空值列

```scala
if (tblNameDst.equals("dm_v_as_djk_info")){
    df = df.drop("bal")
}
else if (tblNameDst.equals("dm_v_as_djkfq_info")){
    df = df.drop("mge_org", "recom_no")
}
else if (tblNameDst.equals("pri_cust_asset_acct_info")){
    df = df.drop("term", "rate", "auto_dp_flg", "matu_date")
}
else if (tblNameDst.equals("pri_cust_liab_acct_info")){
    df = df.drop("grntr_cert_no")
}
```

接着过滤uid为空的数据

```scala
df = df.na.drop(Array("uid"))
```

最后去除重复的行

```scala
df = df.dropDuplicates()
```

>过滤uid为空和去除重复行占用了大量时间


#### 4.1.2.3 数据存入ClickHouse
利用官方驱动`com.github.housepower.jdbc.ClickHouseDriver`完成
```scale=
val write_maps = Map[String, String](
    "batchsize" -> "2000",
    "isolationLevel" -> "NONE",
    "numPartitions" -> "1")
//url
val url = "jdbc:clickhouse://clickhouse:9001/dm"
val dbtable = tblNameDst
val pro = new Properties()
pro.put("driver", "com.github.housepower.jdbc.ClickHouseDriver")
df.write.mode(SaveMode.Append)
        .options(write_maps)
        .option("user", "default")
        .option("password", "{your_password}")
        .jdbc(url, dbtable, pro)
```
### 4.2 效果展示

任务提交至 Spark 后界面展示如图：

![](https://i.imgur.com/SZVR9Z1.png)

运行一次我们的 Spark 任务总用时约为 34min。


## 5. 流式数据部分

### 5.1 代码流程说明
整个流式数据部分的处理分为以下 4 个步骤：
1. 编写 Kafka 生产者将所有流式数据推送到团队搭建的 Kafka 的特定的主题下
2. 编写 Flink Kafka Consumer 消费特定主题下的数据
3. 使用 Flink 算子对数据进行 ETL 
4. 将处理之后的数据 sink 到 ClickHouse 中
#### 5.1.1 Kafka 生产过程

首先通过‘-conf’指定配置文件producer.config,如果配置文件不存在或者没有指定，则退出系统。如果配置了，就开始生产过程。

生产过程的第一步是初始化阶段init。在该阶段中做了两件事：生产者初始化initProducer和数据路径初始化initDataPath。

下面是对以上两个方法的解释：initProducer方法中首先加载了-conf中指定的配置文件，随后又自定义了一些属性，如kafka 集群，broker-list，重试次数，批次大小，等待时间，缓冲区大小等等。最后将所有的配置作为参数构造KafkaProducer对象。initDataPath方法从-conf中指定的配置文件中读取dataPath的值，如果该路径不存在则新建一个目录存放数据。


生产过程的第二步是生产阶段produce。该阶段的任务是遍历dataPath所在目录下的所有文件，按行读取并发送，每发送一万条数据打印当前时间，发送完成立即关闭生产者。其中生产者的主题topic，发送多少数据量的时候才睡眠sleepCounterMax，睡眠时长sleepTime均从配置文件中读取，并可以自定义修改。

核心代码如下：

```java=
public void produce() {
    long sleepCounterMax = Long.parseLong(config.getProperty("sleepCounterMax"));
    long sleepTime = Long.parseLong(config.getProperty("sleepTime"));
    String topic = config.getProperty("topic");
    File[] fileList = dataFile.listFiles();
    int sleep_counter = 0;
    assert fileList != null;
    for (File file : fileList) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String read_in = "";
            long produceCount = 0L;
            while (true) {
                try {
                    if ((read_in = reader.readLine()) != null) {
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic,null, read_in);
                        producer.send(record);
                        if (produceCount % 10000 == 0) {
                            System.out.println("信息数量：" + produceCount + "，当前时间是：" + System.currentTimeMillis());
                        }
                        produceCount++;
                        if (++sleep_counter == sleepCounterMax) {
                            sleep_counter = 0;
                            Thread.sleep(sleepTime);
                        }
                    } else break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    producer.close();
}
```

#### 5.1.2 Flink ETL 过程
我们创建了 `FlinkSinkClickHouse` 类来实现整个 Flink ETL 的逻辑。

首先定义了 `FlinkKafkaConsumer` 用于连接 Kafka 并且消费特定主题下的数据，每次消费指定从上一次消费后的 groupOffset 开始消费，确保不会出现重复消费的问题：
```java=
// 定义 flink kafka consumer
FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(constant.topic, new SimpleStringSchema(), constant.properties);
// 设置 Offset, 防止重复消费 
consumer.setStartFromGroupOffsets();
```
之后我们根据流式数据中的 `eventType` 字段对不同的流式数据进行了分流，保证不同的流式数据可以被并行处理：
```java=
/**
 * 分割数据流 并行处理
 *
 * @param source 数据流
 * @return 分割后每种数据对应的数据流
 */
public List<DataStream<String>> splitDataStream(DataStreamSource<String> source) {
    return Arrays.stream(eventTypes).map(eventType -> source.filter((FilterFunction<String>) s -> {
        try {
            HashMap<String, String> event = JSON.parseObject(s, HashMap.class);
            return event.get("eventType").equalsIgnoreCase(eventType);
        } catch (Exception e) {
            // JSON 错误
            e.printStackTrace();
            return false;
        }
    })).collect(Collectors.toList());
}
```
在分流完之后，我们对每一种数据流都创建了对应的 Flink Operator 实现 ETL。

在设计上，由于每种流式数据都包含许多字段并且有特定的含义，我们设计了能够表征流式数据的 POJO 类封装对应的字段，完成了流式数据的转换(具体的 POJO 类见 pojo/eventbody 包)。

在编写 Flink Operator 时，我们考虑到如果对每种流式数据都编写对应的 operator 进行转换会造成大量的代码冗余，所以我们使用了泛型+反射的方式将流式数据转换成对应的 POJO 类，具体的代码如下：
```java=
/**
 * 创建对应的算子
 */
public <T extends EventBody> SingleOutputStreamOperator<T> createFlinkOperator(DataStream<String> dataStream, Class<T> clazz) {

    MapFunction<String, T> mp = s -> {
        HashMap<String, JSONObject> event = JSON.parseObject(s, HashMap.class);
        String eventBodyStr = event.get("eventBody").toString();
        HashMap<String, String> eventData = JSON.parseObject(eventBodyStr, HashMap.class);
        // 通过反射设置对应的字段
        T eventBody = clazz.newInstance();
        Field[] fields = eventBody.getClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            String fieldName = field.getName();
            if (field.getType() == String.class) {
                field.set(eventBody, eventData.get(fieldName));
            } else if (field.getType() == BigDecimal.class) {
                field.set(eventBody, Convert.toBigDecimal(eventData.get(fieldName), BigDecimal.valueOf(0)));
            } else {
                field.set(eventBody, Convert.toInt(eventData.get(fieldName), 0));
            }
        }
        return eventBody;
    };
    return dataStream.map(mp);
}
```

在使用 Flink Operator 对流式数据完成转换之后，我们对每一种流式数据创建了对应的 SinkFunction 将数据 sink 到 ClickHouse 中。

具体来说，我们继承了 `RichSinkFunction` 这个类，并且重写了其中的 `invoke` 方法将数据最终插入 ClickHouse。在插入数据的时候，我们设置了对应的批量插入数 `INSERT_BATCH_SIZE` 用来提高插入的速度，并且记录响应的插入数据的速率。具体的代码实现如下(以 Contract 流式数据为例)：
```java=
@Override
public void invoke(Contract value, Context context) throws Exception {
    // 具体的sink处理
    ClickHouseProperties properties = new ClickHouseProperties();
    properties.setUser("default");
    properties.setPassword("{your_password}");
    properties.setSessionId("default-session-id");

    ClickHouseDataSource dataSource = new ClickHouseDataSource(Constant.getInstance().url, properties);
    Map<ClickHouseQueryParam, String> additionalDBParams = new HashMap<>();
    additionalDBParams.put(ClickHouseQueryParam.SESSION_ID, "new-session-id");
    try {
        if (connection == null) {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(sql);
        }
        preparedStatement.setString(1, value.getUid());
        preparedStatement.setString(2, value.getContract_no());
        preparedStatement.setString(3, value.getApply_no());
        preparedStatement.setString(4, value.getArtificial_no());
        preparedStatement.setString(5, value.getOccur_date());
        // 省略其他字段，具体参见源码

        preparedStatement.addBatch();

        ++count;
        ++Constant.totalCount;
        if (count % Constant.INSERT_BATCH_SIZE == 0) { //可能会丢最后几条(小于INSERT_BATCH_SIZE条)
            preparedStatement.executeBatch();
            //提交，批量插入数据库中
            connection.commit();
            preparedStatement.clearBatch();
        }
        if (Constant.totalCount % Constant.INSERT_LOG_SIZE == 0) {
            System.out.println(System.currentTimeMillis() + ": " + "共已插入 " + Constant.totalCount + " 条数据");
        }
    } catch (Exception e) {
        e.printStackTrace();
    }
}
```

`INSERT_BATCH_SIZE`过小会减缓插入速度，过大会导致最后收到的数据丢失太多。经过权衡，我们设为了`100`。由于批量插入可能会丢失最后收到的几条数据，这在数据量大的情况下可以忽略不计，但针对数据量小的`个人网银交易`表，我们采取逐条插入的方式以确保完整性。

此外，在编写 SinkFunction 和 POJO 类时，我们设计了 po_generator 和 util_generator 两个python脚本，自动从建表sql和数据字段解释中提取出 POJO 类，并创建对应的 SinkFunction，避免人工繁琐操作。

### 5.2 效果展示


我们根据 Flink 的处理能力控制了消息的推送速度。在一次 Flink Job 中，Kafka 消息的推送速率如下：

![](https://i.imgur.com/w2imFkq.png)

开始部分推送速率维持在 6k/s 左右，后续可能由于主机散热受限，推送速率下降至 4k/s 左右。总推送平均速率为 4.7k/s 左右。

Flink Job 消费端部分的消费速度与推送速度接近。根据我们的观察，在刚开始时消息挤压的总消息量为 60w 条，由于后续生产者推送速度的下降，堆积量并未明显上升，全程维持在 100w 以下的水平。

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

该场景中我们按照日期展示了各消费类型的消费总额，以及在某一时间内手机银行交易数额 TOP 20 的用户，以便于银行根据用户消费水平制定不同的促销策略。

![](https://i.imgur.com/RUjpeie.png)

### 6.2 银行账户信息

该场景中我们展示了所有银行帐户中定期余额和贷款余额的占比，并按照日期展示贷款还本和贷款还息的总额，最后统计了合同的分类等级情况，以便于银行根据当日数据规划资金安排。

![](https://i.imgur.com/BblvomF.png)


## 7. 数据统计

### 静态表部分

总数据量：5871375

| 表名                     | 原始数据量 | 总行数  |
|:------------------------ |:----------:|:-------:|
| dm_v_as_djk_info         |   19320    |  19320  |
| dm_v_as_djkfq_info       |    473     |   473   |
| pri_credit_info          |   38309    |  38309  |
| pri_star_info            |   290658   | 290658  |
| pri_cust_asset_acct_info |   272367   | 263694  |
| pri_cust_asset_info      |   375879   | 344128  |
| pri_cust_base_info       |   376186   | 376186  |
| pri_cust_liab_acct_info  |  4578250   | 3524000 |
| pri_cust_liab_info       |  1230060   | 1012967 |
| pri_cust_contact_info    |    4661    |  1640   |

### 流式数据部分

总数据量：26006376

|        表名         |  总行数  |
|:-------------------:|:--------:|
| dm_v_tr_contract_mx |  165606  |
|   dm_v_tr_djk_mx    |  517732  |
|   dm_v_tr_dsf_mx    |  12612   |
| dm_v_tr_duebill_mx  |  148281  |
|   dm_v_tr_etc_mx    |  370470  |
|   dm_v_tr_grwy_mx   |    19    |
|   dm_v_tr_gzdf_mx   |  583038  |
|  dm_v_tr_huanb_mx   |  129799  |
|  dm_v_tr_huanx_mx   |  256368  |
|    dm_v_tr_sa_mx    | 21996048 |
|   dm_v_tr_sbyb_mx   |  422767  |
|   dm_v_tr_sdrq_mx   |  80814   |
|   dm_v_tr_shop_mx   |  838648  |
|   dm_v_tr_sjyh_mx   |  484174  |
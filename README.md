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
| Dataease*    |                              | 可视化平台，内存占用极大    |



### 3.3 搭建过程

## 4. 数据库表部分 @hgs @zym

### 4.1 代码流程说明

### 4.2 效果展示

任务提交至 Spark 后界面展示如图：

![](https://i.imgur.com/SZVR9Z1.png)


## 5. 流式数据部分 @wpp @hcx

### 5.1 代码流程说明

### 5.2 效果展示

## 6. 可视化部分

package com.nju.allinplantern.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;

/**
 * 整体流程<br>
 * 1. 向 Kafka 特定的主题下导入 JSON 数据<br>
 * 2. 编写 Flink Kafka Consumer 消费主题下的数据<br>
 * 3. 利用 Flink 算子对数据进行 ETL<br>
 * 4. 处理后的数据 sink 到 ClickHouse
 */
public class FlinkSinkClickHouse {
    public static void main(String[] args) {
        // TODO: 1.消费 kafka 2.数据 ETL 3.sink->ck
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置对应的 topic
        String topic = "transaction";
        Properties props = new Properties();
        // kafka server address
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // GROUP_ID 请使用学号，不同组应该使用不同的GROUP。
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "201250038");
        // 防止加入消费者组较晚，导致丢失加入消息队列之前的消息
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"hcplantern\" password=\"16d808ef\";");
        // 定义 flink kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
        consumer.setStartFromGroupOffsets();
        consumer.setStartFromEarliest();
        DataStreamSource<String> source = env.addSource(consumer);
        // TODO: main 函数

    }
}

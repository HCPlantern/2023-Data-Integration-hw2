package com.nju.allinplantern.flink;

import cn.hutool.core.text.CharSequenceUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.nju.allinplantern.flink.pojo.Event;
import com.nju.allinplantern.flink.pojo.eventbody.EventBody;
import com.nju.allinplantern.flink.pojo.eventbody.Shop;
import com.nju.allinplantern.flink.utils.Converter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.HashMap;
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
        SingleOutputStreamOperator<EventBody> dataStream = source.process(new ProcessFunction<String, EventBody>() {
            @Override
            public void processElement(String s, ProcessFunction<String, EventBody>.Context context, Collector<EventBody> collector) throws Exception {
                // 解析消费到的信息
                HashMap map = JSON.parseObject(s, HashMap.class);
                String eventDate = (String) map.get("eventDate");
                String eventType = (String) map.get("eventType");
                String tableName = Converter.eventTypeToTableName(eventType);
                // 得到对应的 eventBody
                EventBody eventBody = (EventBody) ((JSONObject) map.get("eventBody")).toJavaObject(Class.forName("pojo.eventBody." + CharSequenceUtil.upperFirst(eventType)));
                // TODO: 交易数据不合法
                if (!eventBody.isValid()) {
                    return;
                }
                Event event = Event.builder()
                        .eventDate(eventDate)
                        .eventType(eventType)
                        .eventBody(eventBody)
                        .tableName(tableName)
                        .build();
                // TODO: 转换成具体的交易数据
                context.output(new OutputTag<>("shop"), (Shop) eventBody);
            }
        });
        dataStream.getSideOutput(new OutputTag<>("shop"));
    }
}

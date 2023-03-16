package com.nju.allinplantern.flink;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.map.MapUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.nju.allinplantern.flink.pojo.eventbody.*;
import com.nju.allinplantern.flink.utils.HuanbCkUtil;
import com.nju.allinplantern.flink.utils.SjyhCkUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 整体流程<br>
 * 1. 向 Kafka 特定的主题下导入 JSON 数据<br>
 * 2. 编写 Flink Kafka Consumer 消费主题下的数据<br>
 * 3. 利用 Flink 算子对数据进行 ETL<br>
 * 4. 处理后的数据 sink 到 ClickHouse
 */
public class FlinkSinkClickHouse {
    /**
     * 消费的主题
     */
    private static final String topic = "transaction";

    /**
     * 表驱动
     */
    private static final Map<String, Class<? extends EventBody>> typeClazzMap = MapUtil.ofEntries(
            MapUtil.entry("huanb", Huanb.class),
            MapUtil.entry("huanx", Huanx.class),
            MapUtil.entry("sa", Sa.class),
            MapUtil.entry("sbyb", Sbyb.class),
            MapUtil.entry("sdrq", Sdrq.class),
            MapUtil.entry("shop", Shop.class),
            MapUtil.entry("sjyh", Sjyh.class)
    );
    private static final Map<Class<? extends EventBody>, RichSinkFunction> clazzUtilMap = MapUtil.ofEntries(
            MapUtil.entry(Huanb.class, new HuanbCkUtil()),
            MapUtil.entry(Huanx.class, new HuanbCkUtil()),
            MapUtil.entry(Sa.class, new HuanbCkUtil()),
            MapUtil.entry(Sbyb.class, new HuanbCkUtil()),
            MapUtil.entry(Sdrq.class, new HuanbCkUtil()),
            MapUtil.entry(Shop.class, new HuanbCkUtil()),
            MapUtil.entry(Sjyh.class, new SjyhCkUtil())
    );
    private static final String[] eventTypes = {"contract", "djk", "dsf", "duebill", "etc", "grwy", "gzdf", "huanb", "huanx", "sa", "sbyb", "sdrq", "shop", "sjyh"};

    /**
     * 设置 kafka consumer 对应的属性
     *
     * @param propsFile 配置文件属性
     * @return 配置对象
     */
    public Properties setProps(String propsFile) {
        Properties props = new Properties();
        try {
            InputStream in = ClassLoader.getSystemResourceAsStream(propsFile);
            props.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }

    /**
     * 分割数据流 并行处理
     *
     * @param source 数据流
     * @return 分割后每种数据对应的数据流
     */
    public List<DataStream<String>> splitDataStream(DataStreamSource<String> source) {
        return Arrays.stream(eventTypes).map(eventType -> source.filter((FilterFunction<String>) s -> {
            HashMap<String, String> event = JSON.parseObject(s, HashMap.class);
            return event.get("eventType").equalsIgnoreCase(eventType);
        })).collect(Collectors.toList());
    }

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

    public static void main(String[] args) throws Exception {
        // TODO: 1.消费 kafka 2.数据 ETL 3.sink->ck
        FlinkSinkClickHouse flinkSinkClickHouse = new FlinkSinkClickHouse();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置 props
        Properties props = flinkSinkClickHouse.setProps("flink-kafka-consumer.properties");

        // 定义 flink kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        consumer.setStartFromGroupOffsets();
        consumer.setStartFromEarliest();


        // 设置数据源
        DataStreamSource<String> source = env.addSource(consumer);
        // 分流数据源
        List<DataStream<String>> dataStreams = flinkSinkClickHouse.splitDataStream(source);
        // 先测试一下
        for (int i = 0; i < dataStreams.size(); i++) {
            Class<? extends EventBody> eventDataClazz = typeClazzMap.get(eventTypes[i]);
            RichSinkFunction richSinkFunction = clazzUtilMap.get(eventDataClazz);
            SingleOutputStreamOperator<? extends EventBody> eventDataOperator = flinkSinkClickHouse.createFlinkOperator(dataStreams.get(i), eventDataClazz);
            eventDataOperator.addSink(richSinkFunction);
        }
        env.execute();
    }
}
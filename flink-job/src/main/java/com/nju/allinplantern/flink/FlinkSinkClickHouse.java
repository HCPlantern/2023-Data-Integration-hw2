package com.nju.allinplantern.flink;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.map.MapUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.nju.allinplantern.flink.pojo.eventbody.*;
import com.nju.allinplantern.flink.utils.*;
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

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
     * 表驱动
     */
    private static final Map<String, Class<? extends EventBody>> typeClazzMap = MapUtil.ofEntries(
            MapUtil.entry("contract", Contract.class),
            MapUtil.entry("djk", Djk.class),
            MapUtil.entry("dsf", Dsf.class),
            MapUtil.entry("duebill", Duebill.class),
            MapUtil.entry("etc", Etc.class),
            MapUtil.entry("grwy", Grwy.class),
            MapUtil.entry("gzdf", Gzdf.class),
            MapUtil.entry("huanb", Huanb.class),
            MapUtil.entry("huanx", Huanx.class),
            MapUtil.entry("sa", Sa.class),
            MapUtil.entry("sbyb", Sbyb.class),
            MapUtil.entry("sdrq", Sdrq.class),
            MapUtil.entry("shop", Shop.class),
            MapUtil.entry("sjyh", Sjyh.class)
    );
    private static final Map<Class<? extends EventBody>, RichSinkFunction> clazzUtilMap = MapUtil.ofEntries(
            MapUtil.entry(Contract.class, new ContractCkUtil()),
            MapUtil.entry(Djk.class, new DjkCkUtil()),
            MapUtil.entry(Dsf.class, new DsfCkUtil()),
            MapUtil.entry(Duebill.class, new DuebillCkUtil()),
            MapUtil.entry(Etc.class, new EtcCkUtil()),
            MapUtil.entry(Grwy.class, new GrwyCkUtil()),
            MapUtil.entry(Gzdf.class, new GzdfCkUtil()),
            MapUtil.entry(Huanb.class, new HuanbCkUtil()),
            MapUtil.entry(Huanx.class, new HuanxCkUtil()),
            MapUtil.entry(Sa.class, new SaCkUtil()),
            MapUtil.entry(Sbyb.class, new SbybCkUtil()),
            MapUtil.entry(Sdrq.class, new SdrqCkUtil()),
            MapUtil.entry(Shop.class, new ShopCkUtil()),
            MapUtil.entry(Sjyh.class, new SjyhCkUtil())
    );
    private static final String[] eventTypes = {"contract", "djk", "dsf", "duebill", "etc", "grwy", "gzdf", "huanb", "huanx", "sa", "sbyb", "sdrq", "shop", "sjyh"};

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
        FlinkSinkClickHouse flinkSinkClickHouse = new FlinkSinkClickHouse();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Constant constant = Constant.getInstance();
        if (args[0].equals("-conf")) {
            constant.initialize(args[1]);
        }else {
            System.exit(-1);
        }
        // 定义 flink kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(constant.topic, new SimpleStringSchema(), constant.properties);
        consumer.setStartFromGroupOffsets();
        consumer.setStartFromEarliest();


        // 设置数据源
        DataStreamSource<String> source = env.addSource(consumer);
        // 分流数据源
        List<DataStream<String>> dataStreams = flinkSinkClickHouse.splitDataStream(source);
        for (int i = 0; i < dataStreams.size(); i++) {
            Class<? extends EventBody> eventDataClazz = typeClazzMap.get(eventTypes[i]);
            RichSinkFunction richSinkFunction = clazzUtilMap.get(eventDataClazz);
            SingleOutputStreamOperator<? extends EventBody> eventDataOperator = flinkSinkClickHouse.createFlinkOperator(dataStreams.get(i), eventDataClazz);
            eventDataOperator.addSink(richSinkFunction);
        }
        env.execute();
    }
}
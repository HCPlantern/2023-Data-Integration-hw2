package com.nju;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.header.Header;

import javax.swing.plaf.IconUIResource;
import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

/**
 * This Kafka consumer will consume data provided by TAs and save it to txt file.<br>
 * The topic name is "transaction".<br>
 * If data contains specific groupId, it will be saved to another txt file additionally.
 */
public class Consumer {

    Properties props;
    KafkaConsumer<String, String> consumer;
    BufferedWriter fileWriter;
    BufferedWriter additionalFileWriter;

    public Consumer() {
        props = new Properties();
        // kafka server address
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.29.4.17:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // GROUP_ID 请使用学号，不同组应该使用不同的GROUP。
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "201250038");
        // 防止加入消费者组较晚，导致丢失加入消息队列之前的消息
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"student\" password=\"nju2023\";");

        consumer = new KafkaConsumer<>(props);
        // subscribe to "transaction" topic
        consumer.subscribe(Collections.singletonList("transaction"));

        // Initialize file writer
        try {
            String filePath = "kafka-consumer/src/main/resources/kafka/transaction.txt";
            String additionalFilePath = "kafka-consumer/src/main/resources/kafka/transaction_additional.txt";
            File file = new File(filePath);
            File additionalFile = new File(additionalFilePath);
            if (!file.exists()) {
                file.createNewFile();
            }
            if (!additionalFile.exists()) {
                additionalFile.createNewFile();
            }
            fileWriter = new BufferedWriter(new FileWriter(file, true));
            additionalFileWriter = new BufferedWriter(new FileWriter(additionalFile, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void consume() {
        // 会从最新数据开始消费
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                // 获取消息数据
                writeToFile(fileWriter, record.value());
                // 获取消息头
                Header groupIdHeader = record.headers().lastHeader("groupId");
                if (groupIdHeader != null) {
                    byte[] groupId = groupIdHeader.value();
                    // 此处yourGroupId替换成你们组的组号
                    if (Arrays.equals("4".getBytes(), groupId)) {
                        // 额外记录这条数据
                        writeToFile(additionalFileWriter, record.value());
                    }
                }
            }
        }

    }

    /**
     * Append String data to file end. If file not exists, create it.
     *
     * @param data String data to write.
     */

    private void writeToFile(Writer writer, String data) {
        try {
            writer.write(data);
            writer.write(",");
            writer.write(System.lineSeparator());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            fileWriter.close();
            additionalFileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Consumer consumer = new Consumer();
        consumer.consume();
        consumer.close();
    }
}

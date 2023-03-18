package com.nju;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.header.Header;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

/**
 * This Kafka consumer will consume data provided by TAs and save it to txt file.<br>
 * The topic name is "transaction".<br>
 * If data header contains specific groupId, it will be saved to another txt file additionally.
 */
public class Consumer {

    /**
     * The name of the system property that specifies the location of the configuration file.
     */
    private static final String CONF_FILE_PROPERTY_NAME = "consumer.config";

    Properties config;
    KafkaConsumer<String, String> consumer;
    /**
     * The file writer to write data to file.
     */
    BufferedWriter fileWriter;
    /**
     * The file writer to write data to additional file.
     */
    BufferedWriter additionalFileWriter;
    Integer count = 0;

    public Consumer() {
        initConsumer();
        initWriter();
    }

    private void initConsumer() {
        config = new Properties();
        try {
            config.load(Files.newBufferedReader(Paths.get(System.getProperty(CONF_FILE_PROPERTY_NAME))));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Properties props = new Properties();
        // kafka server address
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.29.4.17:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // GROUP_ID 请使用学号，不同组应该使用不同的GROUP。
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getProperty("groupId"));
        // 防止加入消费者组较晚，导致丢失加入消息队列之前的消息
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"student\" password=\"nju2023\";");

        consumer = new KafkaConsumer<>(props);
        // subscribe to "transaction" topic
        consumer.subscribe(Collections.singletonList("transaction"));
    }

    private void initWriter() {
        config = new Properties();
        try {
            config.load(Files.newInputStream(Paths.get(System.getProperty(CONF_FILE_PROPERTY_NAME))));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String groupId = config.getProperty("groupId");
        String filePath = config.getProperty("filePath");
        String additionalFilePath = config.getProperty("additionalFilePath");
        if (groupId == null || filePath == null || additionalFilePath == null) {
            throw new RuntimeException("groupId, filePath, additionalFilePath must be set in consumer.properties");
        }
        try {
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
        count++;
        if (count == 10000) {
            System.out.println(new Date() + ": Receive 1w data.");
            count = 0;
        }
        try {
            writer.write(data);
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
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-conf")) {
                System.setProperty(CONF_FILE_PROPERTY_NAME, args[i + 1]);
            }
        }
        if (System.getProperty(CONF_FILE_PROPERTY_NAME) == null) {
            System.err.println("Please specify the configuration file path by -conf argument.");
            System.exit(1);
        }
        // Initialize consumer
        Consumer consumer = new Consumer();
        consumer.consume();
        consumer.close();
    }
}

package com.nju;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * 整体思路：
 * 1. 将本次作业所需要的数据以json文件的形式存储于data文件夹
 * 2. 对于data文件夹下的文件标识符进行遍历，将所有文件写入到kafka topic中
 * 3. 通过配置linger.ms等数据保障其吞吐量，以及降低磁盘IO负担
 * 4. 使用行计数器间隔性sleep当前线程，以控制生产流量
 * 5. 行计数器兼做分区计数器，用于确保数据在分区的平均分配
 */
public class Producer {

    /**
     * The name of the system property that specifies the location of the configuration file.
     */
    private static final String CONF_FILE_PROPERTY_NAME = "producer.config";
    Properties config;
    KafkaProducer<String, String> producer;
    File dataFile;

    public Producer() {
        initProducer();
        initDataPath();
    }

    private void initProducer() {
        config = new Properties();
        try {
            config.load(Files.newBufferedReader(Paths.get(System.getProperty(CONF_FILE_PROPERTY_NAME))));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Properties props = new Properties();
        //kafka 集群，broker-list
        props.put("bootstrap.servers", "kafka:9092");
        props.put("acks", "0");
        //重试次数
        props.put("retries", 1);
        //批次大小
        props.put("batch.size", 16384);
        //等待时间
        props.put("linger.ms", 200);
        //RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    private void initDataPath() {
        config = new Properties();
        try {
            config.load(Files.newInputStream(Paths.get(System.getProperty(CONF_FILE_PROPERTY_NAME))));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String dataPath = config.getProperty("dataPath");
        if (dataPath == null) {
            throw new RuntimeException("dataPath must be set in producer.properties");
        }
        try {
            dataFile = new File(dataPath);
            if (!dataFile.exists()) {
                dataFile.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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
        new Producer().produce();
    }
}

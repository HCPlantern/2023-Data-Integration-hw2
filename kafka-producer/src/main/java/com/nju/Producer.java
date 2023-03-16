package com.nju;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
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

    public static void main(String[] args) {

        File dataPath = new File("src/main/resources/data");
        File[] tempList = dataPath.listFiles();

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
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
/*        try {
        // 生产单条数据示例
            ProducerRecord<String, String> record = new ProducerRecord<>("dm", "res");
            producer.send(record);
        } finally {
            producer.close();
        }*/

        int sleep_counter=0;
        assert tempList != null;
        for(File f:tempList){
            try{
                FileInputStream fis=new FileInputStream(f);
                BufferedReader br=new BufferedReader(new InputStreamReader(fis));
                String readin="";
                while(true){
                    try{
                        if((readin=br.readLine())!=null){
                            ProducerRecord<String, String> record=new ProducerRecord<>("hgs",0,null, readin);
                            System.out.println(readin);
                            producer.send(record);
                            if(++sleep_counter==1024){
                                sleep_counter=0;
                                Thread.sleep(50);
                            }
                        }else break;
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }catch(FileNotFoundException e){
                e.printStackTrace();
            }
        }
        producer.close();
    }
}

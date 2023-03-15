/**
 * 将本次作业所需要的数据以json文件的形式存储于data文件夹
 * 对于data文件夹下的文件标识符进行遍历，将所有文件写入到kafka topic中
 * 通过配置linger.ms等数据保障其吞吐量，以及降低磁盘IO负担
 * 使用行计数器间隔性sleep当前线程，以控制生产流量
 * 行计数器兼做分区计数器，用于确保数据在分区的平均分配
 */
public static void main(String[]args){
    File dataPath=new File("~/data");
    File[]tempList=dataPath.listFiles();
    Properties kafkaProps=new Properties();
    kafkaProps.put("bootstrap.servers","kafka:9092");
    kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put("linger.ms","2000");
    KafkaProducer<String, String> producer=new KafkaProducer<String, String>(kafkaProps);
    int sleep_counter=0;
    for(File f:tempList){
        try{
            FileInputStream fis=new FileInputStream(f);
            BufferedReader br=new BufferedReader(new InputStreamReader(fis));
            String readin="";
            while(true){
                try{
                    if((readin=br.readLine())!=null){
                        ProducerRecord<String, String> record=new ProducerRecord<>("mxdeal",sleep_counter&7,null,readin);
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

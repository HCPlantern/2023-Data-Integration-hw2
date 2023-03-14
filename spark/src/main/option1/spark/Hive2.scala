package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Hive2 {
  LoggerUtil.setSparkLogLevels()

  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")
    //以local模式部署spark
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[2]")

    //sparksession 读取数据入口
    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    //利用官方hive驱动连接远程hive静态仓库，DataFrameReader
    val reader = session.read.format("jdbc")
      .option("url", "jdbc:hive2://172.29.4.17:10000/default")
      .option("user", "student")
      .option("password", "nju2023")
      .option("driver", "org.apache.hive.jdbc.HiveDriver")
    //val df = reader.option("dbtable", "tableName").load()
    val registerHiveDqlDialect = new RegisterHiveSqlDialect()
    registerHiveDqlDialect.register()

    //todo:对⼗张表进⾏去重和过滤（过滤NULL列）以及数据聚合等ETL操作
    val tblNameDsts = List("dm_v_as_djk_info", "dm_v_as_djkfq_info", "pri_credit_info", "pri_cust_asset_acct_info", "pri_cust_asset_info", " pri_cust_base_info", "pri_cust_contact_info", "pri_cust_liab_info", "pri_star_info", " pri_cust_liab_acct_info")
    for (tblNameDst <- tblNameDsts) {
      var df = reader.option("dbtable", tblNameDst).load()
      // 去掉列名中的表名前缀，方便解析
      val columnNames = df.columns.toList.map(name => name.substring(tblNa
        meDst.length + 1)
      ).toArray
      df = df.toDF(columnNames: _*)
      // 过滤uid为空的数据
      df = df.na.drop(Array("uid"))
      // 去除重复的⾏
      df = df.dropDuplicates()
      df.show(10, false)


      val write_maps = Map[String, String](
        "batchsize" -> "2000",
        "isolationLevel" -> "NONE",
        "numPartitions" -> "1")
      //todo:url
      val url = "jdbc:clickhouse://127.0.0.1:8123/data_integration"
      val dbtable = tblNameDst
      val pro = new Properties()
      pro.put("driver", "cc.blynk.clickhouse.ClickHouseDriver")
      df.write.mode(SaveMode.Append)
        .options(write_maps)
        .jdbc(url, dbtable, pro)
    }

    val tblNameDst = tblNameDsts(6)
    var df = reader.option("dbtable", tblNameDst).load()
    // 去掉列名中的表名前缀，⽅便同学解析
    val columnNames = df.columns.toList.map(name => name.substring(tblNameDst.length + 1)).toArray
    df = df.toDF(columnNames: _*) // 使⽤简化后的列名构建DataFrame
    // 过滤contact ⽆效数据⾏
    df = df.where("contact != '⽆' and contact != '-' and contact != ''")
    df = df.drop("sys_source", "create_date", "update_date") // 去掉不需要显示的列
    df = df.na.drop(List("contact")) // 丢弃含有null或者NAN的⾏
    df = df.dropDuplicates("uid", "contact") // 对于联系⽅式去重uid
    df = df.withColumn("contact_phone", col("con_type") + 1) // 添加新列
    df = df.withColumn("contact_address", col("con_type") + 1)
    // df.rdd：将dataFrame转换成RDD进⾏操作
    // rdd.map(row => 对每⾏数据进⾏操作
    // 得到rdd[k,v]格式：（uid，ListBuffer(con_type,contact,,))
    // ListBuffer: 0:con_type 1:contact 2:contact_phone 3:contact_address ，使⽤逗号分割
    var res = df.rdd.map(row => row.getAs("uid").toString -> ListBuffer[String
    ](row.getAs("con_type").toString, row.getAs("contact").toString, "", ""))
    // 对con_type判断来填充ListBuffer
    res = res.map(item => {
      val listBufferValue = item._2
      if (listBufferValue.head == "TEL" || listBufferValue.head == "OTH" ||
        listBufferValue.head == "MOB") {
        listBufferValue(2) = listBufferValue(1)
      } else {
        listBufferValue(3) = listBufferValue(1)
      }
      item
    })
    // RDD 两两聚合 r1 r2 为key相同的两个value，将r2的信息合并到r1上
    res = res.reduceByKey((r1, r2) => {
      if (r2.head == "TEL" || r2.head == "OTH" || r2.head == "MOB") {
        if (r1(2).nonEmpty) {
          r1(2) = r1(2) + "," + r2(1)
        } else {
          r1(2) = r2(1)
        }
      } else { //r2类型为address
        if (r1(3).nonEmpty) {
          r1(3) = r1(3) + "," + r2(1)
        } else {
          r1(3) = r2(1)
        }
      }
      r1
    })
    val structFields = Array(StructField("uid", StringType, true), StructField("contact_phone", StringType, false), StructField("contact_address", StringType, false))
    val structType = StructType(structFields)
    val rdd = res.map(item => Row(item._1, item._2(2), item._2(3)))
    df = session.createDataFrame(rdd, structType)
    df.show(100, truncate = false)

    session.close()

  }

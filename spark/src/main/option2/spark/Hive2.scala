package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
  1.spark版本变更为2.3.3，部署模式local即可。也可探索其他模式。
  2.由于远程调试出现的各种问题，且远程调试并非作业重点，这里重新建议使用spark-submit方式
  3.本代码及spark命令均为最简单配置。如运行出现资源问题，请根据你的机器情况调整conf的配置以及spark-submit的参数，具体指分配CPU核数和分配内存。

  调试：
    当前代码中集成了spark-sql，可在开发机如windows运行调试;
    需要在开发机本地下载hadoop，因为hadoop基于Linux编写，在开发机本地调试需要其中的一些文件，如模拟Linux目录系统的winutils.exe；
    请修改System.setProperty("hadoop.home.dir", "your hadoop path in windows like E:\\hadoop-x.x.x")

  部署：
    注释掉System.setProperty("hadoop.home.dir", "your hadoop path in windows like E:\\hadoop-x.x.x")；
    修改pom.xml中<scope.mode>compile</scope.mode>为<scope.mode>provided</scope.mode>
    打包 mvn clean package
    上传到你的Linux机器

    注意在~/base_profile文件中配置$SPARK_HOME,并source ~/base_profile,或在bin目录下启动spark-submit
    spark-submit Spark2DB-1.0.jar
 */


object Hive2 {
  // parameters
  LoggerUtil.setSparkLogLevels()

  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4")

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")

    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val reader = session.read.format("jdbc")
      .option("url", "jdbc:hive2://172.29.4.17:10000/default")
      .option("user", "student")
      .option("password", "nju2023")
      .option("driver", "org.apache.hive.jdbc.HiveDriver")
    val registerHiveDqlDialect = new RegisterHiveSqlDialect()
    registerHiveDqlDialect.register()

    //etl小作业 —— pri_cust_contact_info
    val tblNameDsts = List("pri_cust_contact_info")

    for (tblNameDst <- tblNameDsts) {
      var df = reader.option("dbtable", tblNameDst).load()
      // 去掉列名中的表名前缀，方便解析
      val columnNames = df.columns.toList.map(name => name.substring(tblNameDst.length + 1)).toArray
      df = df.toDF(columnNames: _*)
      //首先先过滤掉无用列 sys_source ， create_date ， update_date
      df = df.drop("sys_source", "create_date", "update_date")
      //然后将每一行的数据映射成二元组（因为后续 reduceByKey 只能作用于二元组），该二元组的第一个元素是 uid ，第二个
      //元素是一个嵌套的二元组（依据con_type做映射），该嵌套二元组的内容是 contact_phone 和 contact_address
      df.rdd.map(row => (row.getString(0), (if (row.getString(1).equals("TEL") || row.getString(1).equals("OTH") || row.getString(1).equals("MOB"))
        row.getString(2).trim else "", if (!row.getString(1).equals("TEL") && !row.getString(1).equals("OTH") && !row.getString(1).equals("MOB")) row.getString(2).trim
      else "")))
      //接着进行 reduceByKey操作 ，将任意相同的 uid 的行数据进行合并。这里以合并 contact_phone 列简单介绍一下，如果
      //第一行的 contact_phone 列数据不存在前第二行该列有数据，则直接将 contact_phone 设置为第二行的数据否则则需要考虑
      //在两行数据合并时添加
      reduceByKey((x, y) => {

        var contact_phone = x._1

        var contact_address = x._2
        // x的 contact_phone列为空
        if (contact_phone.equals("") || contact_phone.equals("-") || contact_phone.equals("无") || contact_phone ==
          null) {
          // y的 contact_phone列不为空
          if (!y._1.equals("") && !y._1.equals("-") && !y._1.equals("无") && !(y._1 == null)) {
            contact_phone = y._1
          }
        } else {
          // y的 contact_phone列不为空
          if (!y._1.equals("") && !y._1.equals("-") && !y._1.equals("无") && !contact_phone.contains(y._1) && !(y._1 == null)) {
            contact_phone += "," + y._1
          }
        }
        // x的 contact_phone列为空
        if (contact_address.equals("") || contact_address.equals("-") || contact_address.equals("无") ||
          contact_address == null) {
          // y的 contact_phone列不为空
          if (!y._2.equals("") && !y._2.equals("-") && !y._2.equals("无") && !(y._2 == null)) {
            contact_address = y._2
          }
        } else {
          // y的 contact_phone列不为空
          if (!y._2.equals("") && !y._2.equals("-") && !y._2.equals("无") && !contact_address.contains(y._2) && !(y._2 == null)) {
            contact_address += "," + y._2
          }
        }
        (contact_phone, contact_address)
      }
      )
      //最后需要将合并好的二元组重新映射回三元组并转回DataFrame结构
      map(x => (x._1, x._2._1, x._2._2))
      df = session.createDataFrame(rdd)
      df = df.toDF(Array("uid", "contact_phone", "contact_address"): _*)
      // code
    }
    //其余的表将所有的空值列删除
    case "dm_v_as_djk_info"
    =>
    {
      df = df.drop("bal")
    }
    case "dm_v_as_djkfq_info"
    =>
    {
      df = df.drop("mge_org", "recom_no")
    }
    case "pri_cust_asset_acct_info"
    =>
    {
      df = df.drop("term", "rate", "auto_dp_flg", "matu_date")
    }
    case "pri_cust_liab_acct_info"
    =>
    {
      df = df.drop("grntr_cert_no")
    }
    case _ => {
    }


  }

  //利用第三方驱动 cc.blynk.clickhouse.ClickHouseDriver 完成
  def store_to_clickhouse(df: DataFrame, dbtable: String): Unit = {
    val write_maps = Map[String, String](
      "batchsize" -> "2000"
      ,
      "isolationLevel" -> "NONE"
      ,
      "numPartitions" -> "1"
    )
    val url = "jdbc:clickhouse://192.168.1.24:8123/dm"
    val pro = new Properties()
    pro.put("driver", "cc.blynk.clickhouse.ClickHouseDriver")
    df.write.mode(SaveMode.Append)
      .options(write_maps)
      .jdbc(url, dbtable, pro)
  }

  session.close()
}

}

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

    val tblNameDsts = List("pri_cust_contact_info")

    for (tblNameDst <- tblNameDsts) {
      var df = reader.option("dbtable", tblNameDst).load()
      val columnNames = df.columns.toList.map(name => name.substring(tblNameDst.length + 1)).toArray
      df = df.toDF(columnNames: _*)

      // code
    }
    session.close()
  }

}

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
            .setMaster("local[4]")

        val session = SparkSession.builder()
            .config(conf)
            .getOrCreate()
        //利用官方hive驱动连接远程hive静态仓库
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
            df.rdd.map(row => (row.getString(0),
                                    (if (row.getString(1).equals("TEL") || row.getString(1).equals("OTH") || row.getString(1).equals("MOB"))
            row.getString(2).trim
            else "",
                if (!row.getString(1).equals("TEL") && !row.getString(1).equals("OTH") && !row.getString(1).equals("MOB"))
            row.getString(2).trim
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

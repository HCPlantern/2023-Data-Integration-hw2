package spark

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}

/**
 * 避免内容与列名相同
 */
case object HiveSqlDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:hive2")

  override def quoteIdentifier(colName: String): String = {
    colName.split('.').map(part => s"`$part`").mkString(".")
  }
}

class RegisterHiveSqlDialect {
  def register(): Unit = {
    JdbcDialects.registerDialect(HiveSqlDialect)
  }
}

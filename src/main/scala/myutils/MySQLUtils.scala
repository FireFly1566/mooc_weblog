package myutils

import java.sql.{Connection, DriverManager, PreparedStatement}

/*
* MySQL 操作工具类
* */
object MySQLUtils {
  /*
  * 获取数据库连接
  * */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/weblog?useSSL=false&user=root&password=root")
  }

  /*
  * 释放资源
  * */
  def release(connection: Connection, pstmt: PreparedStatement) = {
    try {
      if (pstmt != null) pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) connection.close()
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }

}

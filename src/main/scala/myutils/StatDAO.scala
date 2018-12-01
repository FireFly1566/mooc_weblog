package myutils

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/*
* 各个维度统计的DAO操作
* */
object StatDAO {
  /*
  * 批量保存 DayVideoAccessStat 到数据库
  * */
  def insertDayVideoAccessStat(list: ListBuffer[DayVideoAccessStat]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false)
      val sql = "insert into day_video_access_topn_stat(day,cms_id,times) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)
      for (ele <- list) {
        /*
        * 批处理使用手动提交
        * */
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)

        pstmt.addBatch()
      }
      pstmt.executeBatch() //执行批量处理
      connection.commit() //手工提交


    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }
}

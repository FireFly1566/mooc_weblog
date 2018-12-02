package myutils

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer


/*
* 数据库建表
* create table day_video_access_topn_stat(
* day varchar(8) not null,
* cms_id bigint(10) not null,
* times bigint(10) not null,
* primary key (day,cms_id)
* );
*
* */

/*
* create table day_video_city_access_topn_stat(
* day varchar(8) not null,
* cms_id bigint(10) not null,
* city varchar(20) not null,
* times bigint(10) not null,
* times_rank int not null,
* primary key (day,cms_id,city)
* );
*
* */

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


  /*
 * 批量保存 DayCityVideoAccessStat 到数据库
 * */
  def insertDayCityVideoAccessStat(list: ListBuffer[DayCityVideoAccessStat]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false)
      val sql = "insert into day_video_city_access_topn_stat(day,cms_id,city,times,times_rank) values (?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)
      for (ele <- list) {
        /*
        * 批处理使用手动提交
        * */
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setString(3, ele.city)
        pstmt.setLong(4,ele.times)
        pstmt.setInt(5,ele.timesRank)

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

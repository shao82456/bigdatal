package core.demo

import java.sql.{Connection, DriverManager}

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/*演示如何使用mapPartitions写数据
* 不能直接写这是由于scala迭代器懒执行的逻辑造成的*/
object WritePartition {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[3]")
    conf.setAppName("demo")
    val sc = SparkContext.getOrCreate(conf)


    val rdd = sc.parallelize(DataGenerator.prepareData(30))
    rdd.cache()
    val savedRdd=rdd.mapPartitions(it=>{
      val conn=getOrCreateConn()
      val it2=write2Mysql2(it,conn)
//      conn.close()
      it2
    })
    println(savedRdd.count())
  }

  def write2Mysql(iterator: Iterator[Student],conn:Connection): Iterator[Student] = {
    val sql = "insert into student values (?,?,?)"
    val stmt =  conn.prepareStatement(sql)
    val res=iterator.map(st=>{
      stmt.setInt(1,st.id)
      stmt.setString(2,st.name)
      stmt.setString(3,st.birthday)
      stmt.addBatch()
      st
    })
    stmt.executeUpdate();
    res
  }

  def write2Mysql2(iterator: Iterator[Student],conn:Connection): Iterator[Student] ={
    val sql = "insert into student values (?,?,?)"
    val stmt =  conn.prepareStatement(sql)
    new Iterator[Student](){
      override def hasNext: Boolean = {
        val res=iterator.hasNext
        if(!res){
          stmt.executeBatch();
          conn.close()
        }
        res
      }

      override def next(): Student = {
        val next=iterator.next()
        stmt.setInt(1,next.id)
        stmt.setString(2,next.name)
        stmt.setString(3,next.birthday)
        stmt.addBatch()
        next
      }
    }
  }

  /**
   * 获得或创建jdbc-mysql连接，关注点不在这里，先用创建连接实现
   *
   * @return
   */
  def getOrCreateConn(): Connection = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"
    val user = "sakura"
    val password = "test"
    Class.forName(driver)
    DriverManager.getConnection(url, user, password)
  }
}
package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sql.core.demo.分组排序.generateData

/**
 * Author: shaoff
 * Date: 2020/7/14 11:28
 * Package: sql
 * Description:
 *
 */
object 分组排序SQL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("分组排序")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val data = generateData(30)
    import spark.implicits._
    val studentsDF = sc.parallelize(data).toDF()

    studentsDF.explain()
    studentsDF.show(100)
    studentsDF.createOrReplaceTempView("students")
    val sort=
      """
        |select name,score,class1,row_number()
        |over (partition by class1 order by score desc)
        |as order
        |from students
        |""".stripMargin

    val sortAndTake=
      """
        |select stu_t1.name,stu_t1.score,stu_t1.class1 from(
        |   select name,score,class1,row_number()
        |   over (partition by class1 order by score desc) as order
        |   from students
        |) as stu_t1
        |where stu_t1.order <=3
        |""".stripMargin

    val df1=spark.sql(sortAndTake).cache()
//    df1.explain()
//    df1.show(100)
//      df1.createOrReplaceTempView("res")
//    spark.catalog.cacheTable("res")
//    spark.sql("select * from res").show(100)


    Thread.sleep(1000*1000)
//    studentsDF.show()
  }

}

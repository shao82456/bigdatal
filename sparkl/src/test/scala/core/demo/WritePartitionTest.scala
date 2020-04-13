//package core.demo
//
//import core.demo.WritePartition.Student
//import org.scalatest.FunSuite
//
///**
// * Author: shaoff
// * Date: 2020/3/7 23:02
// * Package: core.demo
// * Description:
// *
// */
//class WritePartitionTest extends FunSuite {
//
//  test("testWrite2Mysql2") {
//    val data=List(Student(1,"shaofengfeng","1996-09-29"),
//      Student(2,"wangjialiang","1999-03-06"))
//    val it=data.iterator
//    val it2=WritePartition.write2Mysql2(it,WritePartition.getOrCreateConn())
//    println(it2.size)
//  }
//
//}

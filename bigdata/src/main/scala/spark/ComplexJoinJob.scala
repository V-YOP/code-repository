package spark

import org.apache.spark.{SparkConf, SparkContext}

object ComplexJoinJob extends App {
  def someComplexJob(sc : SparkContext, deptInputPath : String, empInputPath : String, outputPath : String) : Unit = {

    val depts = sc.textFile(deptInputPath)
      .map(_.split(","))
      .map{cols => (cols(0), cols)}
    val emps = sc.textFile(empInputPath)
      .map(_.split(","))
      .map{cols => (cols(7), cols)}
    // 值得注意的是，Spark进行join操作不需要
    // 像MapReduce那样在KEY里给定一个虚拟键，
    // 但相信Spark的实现也会采取类似的手段
    val deptEmps = depts.join(emps)

    // 做一个max操作
    // 需注意，reduceByKey中的参数拿不到KEY，这点很好
    // 它怎么推断不出来……
    deptEmps.reduceByKey{ (a, b) =>
      val (pairA@(_, empA), pairB@(_, empB)) = (a, b)
      if (empA(5).toDouble > empB(5).toDouble)
        pairA
      else pairB
    }.map{case (_, (dept, emp)) =>
      Array(dept(1),emp(1),emp(5)).mkString(",")
    }.saveAsTextFile(outputPath)
  }
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("ComplexJoinJob"))
  val deptInputPath +: empInputPath +: outputPath +: _ = args.toSeq
  someComplexJob(sc, deptInputPath, empInputPath, outputPath)
}

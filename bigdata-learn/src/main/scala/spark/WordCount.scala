package spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.Path


/**
 * Spark的第一个示例即WordCount
 */
object WordCount extends App {
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Word Count"))
  val lines = sc.textFile("/Users/aoymykn/CODE/code-repository/bigdata-learn/src/main/resources/tale-of-two-cities.txt")
  val words = lines.flatMap(_.split("[ \\t,.?!'\"]").filter(_.nonEmpty).map((_, 1)))
  val counts = words.reduceByKey(_+_).sortBy(_._2)
  counts.saveAsTextFile("/Users/aoymykn/CODE/code-repository/bigdata-learn/src/main/resources/output")
}

package spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.Path


/**
 * Spark的第一个示例即WordCount
 */
object WordCount extends App {
  def wordCount(sc : SparkContext, inputPath : String, outputPath : String) : Unit = {
    val lines = sc.textFile(inputPath)
    val wordPairs = lines.flatMap(_.split(" ")).map((_, 1))
    val resultPairs = wordPairs.foldByKey(0)(_ + _)
    resultPairs.saveAsTextFile(outputPath)
  }
  wordCount(new SparkContext(new SparkConf().setAppName("Word Count")), "file:///share/state.txt", "file:///share/output")
}

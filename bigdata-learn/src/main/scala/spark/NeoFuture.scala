package spark

import org.apache.spark.{SparkConf, SparkContext}

object NeoFuture extends App {
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("WordCOunt"))
  val lines = sc.textFile("/Volumes/Untitled/data/input")
  val words = lines.flatMap(_.split(" "))
  val wordGroups = words.groupBy(identity)
  val count = wordGroups.mapValues(_.size)


}

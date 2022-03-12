import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source.fromResource
import scala.util.Random

object Main extends App {
  val aloha = """0 0 0
                |0 0 1
                |0 1 0
                |0 1 1
                |1 0 0
                |1 0 1
                |1 1 0
                |1 1 1""".stripMargin

  println(Random.shuffle(aloha.split("\n").toSeq).mkString("\n"))
}
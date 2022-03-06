import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ArrayWritable, IntWritable, LongWritable, Text, Writable}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.util.GenericOptionsParser

import java.lang

case class IntArrayWritable() extends ArrayWritable(classOf[IntWritable]) {
}
object IntArrayWritable {
  def apply(values : Array[Writable]) : IntArrayWritable = {
    val res = new IntArrayWritable
    res.set(values)
    res
  }
}

// Mapper的输出为(月份， 标识符)
class AggregationMapper extends Mapper[LongWritable, Text, Text, IntArrayWritable] {

  // 为了让代码业务逻辑更明显，不使用类变量存储输出的Writable对象了
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntArrayWritable]#Context): Unit = {
    import AirlineCol._
    if (value.toString.startsWith("Year")) {
      return
    }
    val colGetter = AirlineCol.build(value.toString.split(","))

    // Scala默认居然没有提供padLeft……真的假的？
    def padLeft(str : String, len : Int, c : Char) : String =
      str.reverse.padTo(len, c).reverse

    val month = padLeft(colGetter(Month), 2, '0')
    val arrDelay = colGetter(ArrDelay).toIntOption.getOrElse(0)
    val depDelay = colGetter(DepDelay).toIntOption.getOrElse(0)
    val isCancelled = colGetter(Cancelled) == "1"
    val isDiverted = colGetter(Diverted) == "1"

    context.write(new Text(month), IntArrayWritable(Array(
      1, if (arrDelay > 0) 1 else 0, if (depDelay > 0) 1 else 0, if (isCancelled) 1 else 0, if (isDiverted) 1 else 0
    ).map(new IntWritable(_))))
  }
}

class AggregationCombiner extends Reducer[Text, IntArrayWritable, Text, IntArrayWritable] {
  override def reduce(key: Text, values: lang.Iterable[IntArrayWritable], context: Reducer[Text, IntArrayWritable, Text, IntArrayWritable]#Context): Unit = {
    import scala.jdk.CollectionConverters.IterableHasAsScala
    val result = values.asScala.map(_.get).foldLeft(Array(0,0,0,0,0)) {(acc, cols) =>
      cols.map(_.asInstanceOf[IntWritable].get).zip(acc).map {case (a, b) => a + b}
    }
    context.write(key, IntArrayWritable(result.map(new IntWritable(_))))
  }
}

class AggregationReducer extends Reducer[Text, IntArrayWritable, Text, Text] {
  override def reduce(key: Text, values: lang.Iterable[IntArrayWritable], context: Reducer[Text, IntArrayWritable, Text, Text]#Context): Unit = {
    // 将Java的Iterable转换为Scala的Iterable
    import scala.jdk.CollectionConverters.IterableHasAsScala

    val Array(recordCount,arrDelayCount,depDelayCount,cancelCount,divertCount) = values.asScala.map(_.get).foldLeft(Array(0,0,0,0,0)) {(acc, cols) =>
      cols.map(_.asInstanceOf[IntWritable].get).zip(acc).map {case (a, b) => a + b}
    }.map(_.toDouble)

    val result = recordCount.toString +"," + List(
      1 - arrDelayCount / recordCount, // 准时到达
      arrDelayCount / recordCount, // 延迟到达
      1 - depDelayCount / recordCount, // 准时出发
      depDelayCount / recordCount, // 延迟出发
      cancelCount / recordCount, // 取消
      divertCount / recordCount // 改航
    ).map(s => (s * 100).toString.take(5) + "%").mkString(",") // 格式化一下
    context.write(key, new Text(result))
  }
}

/**
 * 演示GROUP BY子句的使用
 */
object AggregationMRJob {
  def main(args: Array[String]): Unit = {
    // 1. 根据args初始化GenericOptionParser
    val parser = new GenericOptionsParser(args)
    // 2. 获取输入，输出路径
    val Seq(inputPath, outputPath) = parser.getRemainingArgs.toSeq.map(new Path(_))
    // 3. 获取Configuration，构造Job
    val job = Job.getInstance(parser.getConfiguration)
    job setJarByClass AggregationMRJob.getClass
    // 4. 设置InputFormat, OutputFormat
    job setInputFormatClass classOf[TextInputFormat]
    job setOutputFormatClass classOf[TextOutputFormat[Text, Text]]
    // 5. 设置Mapper和Reducer的输出类型
    job setMapOutputKeyClass classOf[Text]
    job setMapOutputValueClass classOf[IntArrayWritable]
    job setOutputKeyClass classOf[Text]
    job setOutputValueClass classOf[Text]
    // 6. 设置Mapper，Reducer
    job setMapperClass classOf[AggregationMapper]
    job setCombinerClass classOf[AggregationCombiner]
    job setReducerClass classOf[AggregationReducer]
    job setNumReduceTasks 2 // 默认值
    // 7. 设置输入输出路径
    FileInputFormat.setInputPaths(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)
    // 8. 启动
    System exit {
      if (job waitForCompletion true) 0
      else 1
    }
  }
}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper}
import org.apache.hadoop.util.GenericOptionsParser


class WhereClauseMapper extends Mapper[LongWritable, Text, NullWritable, Text] {
  private var delayInMinutes: Int = 0

  private val outputK = NullWritable.get
  private val outputV = new Text()

  override def setup(context: Mapper[LongWritable, Text, NullWritable, Text]#Context): Unit = {
    delayInMinutes = context.getConfiguration.getInt("map.where.delay", 1)
  }

  // 只能说……写得很痛苦
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, NullWritable, Text]#Context): Unit = {
    if (value.toString.startsWith("Year")) {
      return
    }
    // 重用SelectClause的映射操作
    val cols = SelectClause.parse(value.toString).split(",")

    // 有一些NA的情况
    val depDel = cols(8).toIntOption.getOrElse(0)
    val arrDel = cols(9).toIntOption.getOrElse(0)

    if (depDel < delayInMinutes && arrDel < delayInMinutes) {
      return
    }

    val resultStr = cols.mkString(",") + {
      if (depDel >= delayInMinutes && arrDel >= delayInMinutes) ",B"
      else if (depDel >= delayInMinutes) ",O"
      else if (arrDel >= delayInMinutes) ",D"
      else ""
    }

    outputV.set(resultStr)
    context.write(outputK, outputV)
  }

}

object WhereClause {
  def main(args: Array[String]): Unit = {
    // 使用Hadoop提供的工具来解析命令行
    // 实际上这才是Driver类编写的最佳实践
    val parser = new GenericOptionsParser(args)
    // 解构出前两个元素作为输入和输出路径
    val Seq(inputPath, outputPath) = parser.getRemainingArgs.toSeq.map(new Path(_))
    val job = Job.getInstance(parser.getConfiguration)

    job.setJarByClass(WhereClause.getClass)
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[NullWritable, Text]])
    job.setMapOutputKeyClass(classOf[NullWritable])
    job.setMapOutputValueClass(classOf[NullWritable])

    job.setMapperClass(classOf[WhereClauseMapper])
    job.setNumReduceTasks(0)

    FileInputFormat.setInputPaths(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)

    System.exit {
      if (job.waitForCompletion(true)) 0
      else 1
    }
  }
}
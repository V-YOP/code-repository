package mapreduce

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.GenericOptionsParser

import java.io.FileWriter
import java.lang
import scala.util.Random

// 在特定文件夹里创建16个文件，每个文件里包含500万个数
// 总共约 1520M 大小
object CreateInputFiles extends App {
  val path = "/路径/到/输入路径"
  (0 until 16).foreach {i =>
    val writer = new FileWriter(s"$path/$i.txt")
    (0 until 5000000).foreach(_ => writer.write(Math.abs(Random.nextLong()) + "\n"))
    writer.close()
  }
}

class DistributedSortMapper extends Mapper[LongWritable, Text, LongWritable, NullWritable] {
  val outputK = new LongWritable()
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, LongWritable, NullWritable]#Context): Unit = {
    value.toString.toLongOption match {
      case None => {}
      case Some(longValue) => {
        outputK.set(longValue)
        context.write(outputK, NullWritable.get)
      }
    }
  }
}

class DistributedSortReducer extends Reducer[LongWritable, NullWritable, LongWritable, NullWritable] {
  override def reduce(key: LongWritable, values: lang.Iterable[NullWritable], context: Reducer[LongWritable, NullWritable, LongWritable, NullWritable]#Context): Unit = {
    // 为什么要调用forEach呢？因为可能有多个值相等，这时候它们会被分在一个组里
    // 直接打印到结果文件里，那就只留下了不重复的了
    values.forEach(_ => context.write(key, NullWritable.get))
  }
}


object DistributedSortJob extends App {
  val parser = new GenericOptionsParser(args)
  val job = Job.getInstance(parser.getConfiguration)
  val inputPath +: outputPath +: _ = parser.getRemainingArgs.toSeq.map(new Path(_))

  job.setInputFormatClass(classOf[TextInputFormat])
  job.setOutputKeyClass(classOf[TextOutputFormat[_,_]])
  job.setMapOutputKeyClass(classOf[LongWritable])
  job.setMapOutputValueClass(classOf[NullWritable])
  job.setOutputKeyClass(classOf[LongWritable])
  job.setOutputValueClass(classOf[NullWritable])

  job.setMapperClass(classOf[DistributedSortMapper])
  job.setReducerClass(classOf[DistributedSortReducer])
  FileInputFormat.setInputPaths(job, inputPath)
  FileOutputFormat.setOutputPath(job, outputPath)

  System.exit(if (job.waitForCompletion(true)) 0 else 1)
}

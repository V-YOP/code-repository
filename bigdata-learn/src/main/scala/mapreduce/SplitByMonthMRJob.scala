package mapreduce

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}
import org.apache.hadoop.util.GenericOptionsParser

import java.lang

class SplitByMonthMapper extends Mapper[LongWritable, Text, Text, Text] {
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    if (value.toString.startsWith("Year"))
      return
    val colGetter = AirlineCol.build(value.toString.split(","))
    val month = colGetter(AirlineCol.Month)
    context.write(new Text(month), value)
  }
}

class MonthPartitioner extends Partitioner[Text, Text] {
  override def getPartition(key: Text, value: Text, numPartitions: Int): Int =
    key.toString.toInt - 1
}

class SplitByMonthReducer extends Reducer[Text, Text, NullWritable, Text] {
  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, NullWritable, Text]#Context): Unit = {
    // do Nothing!
    values.forEach(context.write(NullWritable.get, _))
  }
}

object SplitByMonthMRJob {
  def main(args: Array[String]): Unit = {
    val parser = new GenericOptionsParser(args)
    val inputPath +: outputPath +: _ = parser.getRemainingArgs.toSeq.map(new Path(_))

    val job = Job.getInstance(parser.getConfiguration)
    job.setJarByClass(SplitByMonthMRJob.getClass)
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_,_]])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])

    job.setMapperClass(classOf[SplitByMonthMapper])
    job.setReducerClass(classOf[SplitByMonthReducer])
    job.setNumReduceTasks(12)
    job.setPartitionerClass(classOf[MonthPartitioner])

    FileInputFormat.setInputPaths(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)
    System.exit {
      if (job.waitForCompletion(true)) 0
      else 1
    }
  }
}

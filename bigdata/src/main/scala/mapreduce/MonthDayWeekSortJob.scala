package mapreduce

import mapreduce.DistributedSortJob.args
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io
import org.apache.hadoop.io.{IntWritable, LongWritable, NullWritable, Text, Writable, WritableComparable}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}
import org.apache.hadoop.util.GenericOptionsParser

import java.io.{DataInput, DataOutput}
import java.lang

// Scala定义这玩意可太难受了，不能用类参数…否则Scala会创建出带参数的构造器，没法用反射创建了
// 键类型
class MonthDayWeek extends WritableComparable[MonthDayWeek] {
  var month : IntWritable = new IntWritable(0)
  var dayOfWeek : IntWritable = new IntWritable(0)

  override def write(out: DataOutput): Unit = {
    month.write(out)
    dayOfWeek.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    month.readFields(in)
    dayOfWeek.readFields(in)
  }

  override def compareTo(o: MonthDayWeek): Int = {
    if (this.month.get == o.month.get) // 如果月份相等，则逆序比较dayOfWeek
      -1 * dayOfWeek.compareTo(o.dayOfWeek)
    else
      this.month.compareTo(o.month)
  }

  // 自动生成
  def canEqual(other: Any): Boolean = other.isInstanceOf[MonthDayWeek]

  override def equals(other: Any): Boolean = other match {
    case that: MonthDayWeek =>
      (that canEqual this) &&
        month == that.month &&
        dayOfWeek == that.dayOfWeek
    case _ => false
  }
  override def hashCode(): Int = {
    val state = Seq(month, dayOfWeek)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

class DelaysWritable extends Writable {
  var year = new IntWritable(0)
  var month = new IntWritable(0)
  var date = new IntWritable(0)
  var dayOfWeek = new IntWritable(0)
  var arrDelay = new IntWritable(0)
  var depDelay = new IntWritable(0)
  var originAirportCode = new Text()
  var destAirportCode = new Text()
  var carrierCode = new Text()

  override def write(out: DataOutput): Unit = {
    year.write(out)
    month.write(out)
    date.write(out)
    dayOfWeek.write(out)
    arrDelay.write(out)
    depDelay.write(out)
    originAirportCode.write(out)
    destAirportCode.write(out)
    carrierCode.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    year.readFields(in)
    month.readFields(in)
    date.readFields(in)
    dayOfWeek.readFields(in)
    arrDelay.readFields(in)
    depDelay.readFields(in)
    originAirportCode.readFields(in)
    destAirportCode.readFields(in)
    carrierCode.readFields(in)
  }


  override def toString = s"$year, $month, $date, $dayOfWeek, $arrDelay, $depDelay, $originAirportCode, $destAirportCode, $carrierCode"
}

class MonthDayWeekSortMapper extends Mapper[LongWritable, Text, MonthDayWeek, DelaysWritable] {
  val outputK = new MonthDayWeek
  val outputV = new DelaysWritable
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, MonthDayWeek, DelaysWritable]#Context): Unit = {
    if (value.toString.startsWith("Year")) return

    import AirlineCol._
    val colGetter = AirlineCol.build(value.toString.split(","))

    // 尽量省事
    try {
      outputK.month = new IntWritable(colGetter(Month).toInt)
      outputK.dayOfWeek = new IntWritable(colGetter(DayOfWeek).toInt)

      outputV.year = new IntWritable(colGetter(Year).toInt)
      outputV.month = new IntWritable(colGetter(Month).toInt)
      outputV.dayOfWeek = new IntWritable(colGetter(DayOfWeek).toInt)
      outputV.date = new IntWritable(colGetter(DayofMonth).toInt)
      outputV.arrDelay = new IntWritable(colGetter(ArrDelay).toInt)
      outputV.depDelay = new IntWritable(colGetter(DepDelay).toInt)
      outputV.destAirportCode = new Text(colGetter(Dest))
      outputV.originAirportCode = new Text(colGetter(Origin))
      outputV.carrierCode = new Text(colGetter(UniqueCarrier))
    } catch {
      case _: Throwable => {return }
    }


    context.write(outputK, outputV)
  }
}

class MonthDayWeekSortReducer extends Reducer[MonthDayWeek, DelaysWritable, DelaysWritable, NullWritable] {
  override def reduce(key: MonthDayWeek, values: lang.Iterable[DelaysWritable], context: Reducer[MonthDayWeek, DelaysWritable, DelaysWritable, NullWritable]#Context): Unit = {
    values.forEach(context.write(_, NullWritable.get))
  }
}

// 继承Configurable接口后，就能够拿到配置信息了，通过key.range指定配置范围
// 用户给定一个key.range，表示每个Reducer将要接受的KEY的数量，如果超出，则最后一个reducer照单全收
// 比如，设置key.range = 24，设置Reducer为3个，则第一个Reducer处理0-23，第二个处理24-48，第三个处理49-83
class MonthDayWeekSortJobPartitioner extends Partitioner[MonthDayWeek, DelaysWritable] with Configurable {
  var indexRange : Int = 84 / 7;
  var config : Configuration = new Configuration();
  override def setConf(conf: Configuration): Unit = {
    config = conf
  }
  override def getConf: Configuration = config
  override def getPartition(key: MonthDayWeek, value: DelaysWritable, numPartitions: Int): Int = {
    val index = ((key.month.get - 1) * 7 + (key.dayOfWeek.get - 1)) / config.getInt("key.range", 84)

    if (index < numPartitions)
      index
    else
      numPartitions - 1
  }
}

object MonthDayWeekSortJob extends App {
  val parser = new GenericOptionsParser(args)
  val job = Job.getInstance(parser.getConfiguration)
  val inputPath +: outputPath +: _ = parser.getRemainingArgs.toSeq.map(new Path(_))

  job.setInputFormatClass(classOf[TextInputFormat])
  job.setOutputKeyClass(classOf[TextOutputFormat[_,_]])
  job.setMapOutputKeyClass(classOf[MonthDayWeek])
  job.setMapOutputValueClass(classOf[DelaysWritable])
  job.setOutputKeyClass(classOf[DelaysWritable])
  job.setOutputValueClass(classOf[NullWritable])

  job.setMapperClass(classOf[MonthDayWeekSortMapper])
  job.setReducerClass(classOf[MonthDayWeekSortReducer])
  job.setPartitionerClass(classOf[MonthDayWeekSortJobPartitioner])
  FileInputFormat.setInputPaths(job, inputPath)
  FileOutputFormat.setOutputPath(job, outputPath)

  System.exit(if (job.waitForCompletion(true)) 0 else 1)
}

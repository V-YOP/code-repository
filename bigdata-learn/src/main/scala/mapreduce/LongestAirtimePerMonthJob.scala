package mapreduce

import mapreduce.SecondarySortJob.args
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io
import org.apache.hadoop.io.{IntWritable, LongWritable, NullWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.GenericOptionsParser

import java.io.{DataInput, DataOutput}
import java.lang
import scala.util.{Random, Try}

class CombineKey extends WritableComparable[CombineKey] {
  var month : Int = 0
  var tailNum : String = ""
  var allDelay : Int = 0

  override def write(out: DataOutput): Unit = {
    out.writeInt(month)
    out.writeUTF(tailNum)
    out.writeInt(allDelay)
  }

  override def readFields(in: DataInput): Unit = {
    month = in.readInt()
    tailNum = in.readUTF()
    allDelay = in.readInt()
  }

  override def compareTo(o: CombineKey): Int = {
    if (month != o.month)
      month.compareTo(o.month)
    else if (tailNum != o.tailNum)
      tailNum.compareTo(o.tailNum)
    else -1 * allDelay.compareTo(o.allDelay)
  }



  override def toString = s"month=$month, tailNum=$tailNum, allDelay =$allDelay"

  def canEqual(other: Any): Boolean = other.isInstanceOf[CombineKey]

  override def equals(other: Any): Boolean = other match {
    case that: CombineKey =>
      (that canEqual this) &&
        month == that.month &&
        tailNum == that.tailNum
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(month, tailNum)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

class LongestAirtimePerMonthMapper extends Mapper[LongWritable, Text, CombineKey, Text] {
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, CombineKey, Text]#Context): Unit = Try {
    if (value.toString.startsWith("Year")) return

    val colGetter = AirlineCol.build(value.toString.split(","))
    val combineKey = new CombineKey

    combineKey.month = colGetter(AirlineCol.Month).toInt
    combineKey.tailNum = colGetter(AirlineCol.TailNum)
    combineKey.allDelay =
      colGetter(AirlineCol.ArrDelay).toIntOption.getOrElse(0)
    + colGetter(AirlineCol.DepDelay).toIntOption.getOrElse(0)
    context.write(combineKey, value)
  }
}

class LongestAirtimePerMonthReducer extends Reducer[CombineKey, Text, Text, Text] {
  override def reduce(key: CombineKey, values: lang.Iterable[Text], context: Reducer[CombineKey, Text, Text, Text]#Context) = {
    // 在这里使用take函数会得到奇怪的结果
    var counter = 0
    val iter = values.iterator()
    while (counter < 10 && iter.hasNext) {
      counter += 1
      context.write(new Text(key.toString), iter.next())
    }
  }
}

class LongestAirtimePerMonthGroupComparator extends WritableComparator(classOf[CombineKey], true) {
  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = Try {
    val l = a.asInstanceOf[CombineKey]
    val r = b.asInstanceOf[CombineKey]

    if (l.month != r.month)
      l.month.compareTo(r.month)
    else l.tailNum.compareTo(r.tailNum)
  }.getOrElse(-1)
}

object LongestAirtimePerMonthJob extends App {
  val parser = new GenericOptionsParser(args)
  val job = Job.getInstance(parser.getConfiguration)
  val inputPath +: outputPath +: _ = parser.getRemainingArgs.toSeq.map(new Path(_))

  job.setInputFormatClass(classOf[TextInputFormat])
  job.setOutputKeyClass(classOf[TextOutputFormat[_,_]])
  job.setMapOutputKeyClass(classOf[CombineKey])
  job.setMapOutputValueClass(classOf[Text])
  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[Text])

  job.setMapperClass(classOf[LongestAirtimePerMonthMapper])
  job.setReducerClass(classOf[LongestAirtimePerMonthReducer])
  job.setGroupingComparatorClass(classOf[LongestAirtimePerMonthGroupComparator])

  FileInputFormat.setInputPaths(job, inputPath)
  FileOutputFormat.setOutputPath(job, outputPath)

  System.exit(if (job.waitForCompletion(true)) 0 else 1)
}



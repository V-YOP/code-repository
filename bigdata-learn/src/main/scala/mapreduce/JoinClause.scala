package mapreduce

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.GenericOptionsParser

import java.io.{DataInput, DataOutput}
import java.lang
import scala.util.Try

// Scala可以自定义自己的构造器……我之前好像没学过这个
// 但是为了能够使用这鬼畜的序列化、反序列化框架，必须使用var定义，我麻了好嘛
case class KeyPair(var stationID : String, var virtualKey : Int) extends WritableComparable[KeyPair] {
  def this() = this("", -1)
  override def compareTo(o: KeyPair): Int = {
    if (stationID != o.stationID)
      stationID.compareTo(o.stationID)
    else virtualKey.compareTo(o.virtualKey)
  }

  override def write(out: DataOutput): Unit = {
    out.writeUTF(stationID)
    out.writeInt(virtualKey)
  }

  override def readFields(in: DataInput): Unit = {
    stationID = in.readUTF()
    virtualKey = in.readInt()
  }
}

class JoinClauseMapper extends Mapper[LongWritable, Text, KeyPair, Text] {
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, KeyPair, Text]#Context): Unit = {
    val cols = value.toString.split(",")
    // 如果有两列，说明是Station表，虚拟键为0，否则是记录表，虚拟键为1
    context.write(KeyPair(cols(0), if (cols.length == 2) 0 else 1), value)
  }
}

class JoinClauseReducer extends Reducer[KeyPair, Text, Text, NullWritable] {
  override def reduce(key: KeyPair, values: lang.Iterable[Text], context: Reducer[KeyPair, Text, Text, NullWritable]#Context): Unit = {
    val iter = values.iterator()

    // 使用Try的上下文——如果第一列不是站点（模式匹配会失败），则直接抛异常跑路
    for {
      firstElem <- Try(iter.next())
      Array(stationId, stationName) = firstElem.toString.split(",")
    } yield iter.forEachRemaining { elem =>
        val Array(_, timeStamp, temperature) = elem.toString.split(",")
        context.write(
          new Text(
            Array(stationId,
            stationName,
            timeStamp,
            temperature).mkString(",")),
          NullWritable.get)
      }
  }
}

// 考虑到是使用整个KEY进行排序，这里不用自定义SortComparator，让它调用KEY里定义的compareTo方法即可
class JoinClauseGroupComparator extends WritableComparator(classOf[KeyPair], true) {
  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    // 犯了非常愚蠢的错误……变量名写错了，debug了半个小时
    val l = a.asInstanceOf[KeyPair]
    val r = b.asInstanceOf[KeyPair]
    l.stationID.compareTo(r.stationID)
  }
}

object JoinClause extends App {
  val parser = new GenericOptionsParser(args)
  val inputPath +: outputPath +: _ = parser.getRemainingArgs.toSeq.map(new Path((_)))
  val job = Job.getInstance(parser.getConfiguration)
  job.setInputFormatClass(classOf[TextInputFormat])
  job.setOutputFormatClass(classOf[TextOutputFormat[_,_]])
  job.setMapOutputKeyClass(classOf[KeyPair])
  job.setMapOutputValueClass(classOf[Text])
  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[NullWritable])
  job.setMapperClass(classOf[JoinClauseMapper])
  job.setReducerClass(classOf[JoinClauseReducer])

  job.setGroupingComparatorClass(classOf[JoinClauseGroupComparator])

  FileInputFormat.setInputPaths(job, inputPath)
  FileOutputFormat.setOutputPath(job, outputPath)
  System.exit {
    if (job.waitForCompletion(true)) 0
    else 1
  }
}

package mapreduce

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text, Writable, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.util.GenericOptionsParser

import java.io.{DataInput, DataOutput}
import java.lang

/**
 * 用以测试一些二次排序的特性
 */
class IntTripleWritable extends WritableComparable[IntTripleWritable] {
  var _1 : Int = -1
  var _2 : Int = -2
  var _3 : Int = -3

  override def write(out: DataOutput): Unit = {
    out.writeInt(_1)
    out.writeInt(_2)
    out.writeInt(_3)
  }

  override def readFields(in: DataInput): Unit = {
    _1 = in.readInt()
    _2 = in.readInt()
    _3 = in.readInt()
  }

  override def compareTo(o: IntTripleWritable): Int = {
    if (_1 != o._1)
      _1 compareTo o._1
    else if (_2 != o._2)
      _2 compareTo o._2
    else _3 compareTo o._3
  }


  override def toString = s"${_1}, ${_2}, ${_3}"
}

class SecondarySortMapper extends Mapper[LongWritable, Text, IntTripleWritable, NullWritable] {
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, IntTripleWritable, NullWritable]#Context): Unit = {
    val Array(_1,_2,_3) = value.toString.split(" ").map(_.toInt)
    val triple = new IntTripleWritable
    triple._1 = _1
    triple._2 = _2
    triple._3 = _3
    context.write(triple, NullWritable.get)
  }
}


class SecondarySortReducer extends Reducer[IntTripleWritable, NullWritable, Text, NullWritable] {
  override def reduce(key: IntTripleWritable, values: lang.Iterable[NullWritable], context: Reducer[IntTripleWritable, NullWritable, Text, NullWritable]#Context): Unit = {
    context.write(new Text("分组，key:" + key.toString), NullWritable.get)
    values.forEach(_=>context.write(new Text(key.toString), NullWritable.get))
  }
}

class SecondarySortSortComparator extends  WritableComparator(classOf[IntTripleWritable], true) {
  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    try {
      val l = a.asInstanceOf[IntTripleWritable]
      val r = b.asInstanceOf[IntTripleWritable]


      if (l._1 != r._1)
        l._1 compareTo r._1
      else l._2 compareTo r._2

    } catch { case _ : Throwable => -1}
  }
}


class SecondarySortGroupComparator extends WritableComparator(classOf[IntTripleWritable], true) {
  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    try {
      val l = a.asInstanceOf[IntTripleWritable]
      val r = b.asInstanceOf[IntTripleWritable]

       l._2 compareTo r._2

    } catch { case _ : Throwable => -1}
  }
}


case object SecondarySortJob extends App {
  val parser = new GenericOptionsParser(args)
  val job = Job.getInstance(parser.getConfiguration)
  val inputPath +: outputPath +: _ = parser.getRemainingArgs.toSeq.map(new Path(_))

  job.setInputFormatClass(classOf[TextInputFormat])
  job.setOutputKeyClass(classOf[TextOutputFormat[_,_]])
  job.setMapOutputKeyClass(classOf[IntTripleWritable])
  job.setMapOutputValueClass(classOf[NullWritable])
  job.setOutputKeyClass(classOf[IntTripleWritable])
  job.setOutputValueClass(classOf[NullWritable])

  job.setMapperClass(classOf[SecondarySortMapper])
  job.setReducerClass(classOf[SecondarySortReducer])
  job.setSortComparatorClass(classOf[SecondarySortSortComparator])
  job.setGroupingComparatorClass(classOf[SecondarySortGroupComparator])

  FileInputFormat.setInputPaths(job, inputPath)
  FileOutputFormat.setOutputPath(job, outputPath)

  System.exit(if (job.waitForCompletion(true)) 0 else 1)
}

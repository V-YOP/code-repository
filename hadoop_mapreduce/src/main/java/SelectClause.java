import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringJoiner;

/**
 * 《深入理解Hadoop》的第一个示例，演示Select子句的使用
 */
public class SelectClause {

    public static class SelectClauseMapper
            // 输出的数据不需要主键，也不需要排序，所以使用NullWritable
            // 如果使用其它的类型，则每行开头都会有一个\t
            extends Mapper<LongWritable, Text, NullWritable, Text> {
        private static final Text outputV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 表头
            if (value.toString().startsWith("Year")) {
                return;
            }

            String result = parse(value.toString());
            outputV.set(result);

            context.write(NullWritable.get(), outputV);
        }
    }

    public static String parse(String value) {
        AirlineCol.ColGetter cols = AirlineCol.build(value.split(","));

        // 很壮观
        String date = new StringJoiner("/")
                .add(cols.apply(AirlineCol.Year))
                .add(StringUtils.leftPad(cols.apply(AirlineCol.Month), 2, "0"))
                .add(StringUtils.leftPad(cols.apply(AirlineCol.DayofMonth), 2, "0"))
                .toString();
        String dayOfWeek = cols.apply(AirlineCol.DayOfWeek);
        String depTime = StringUtils.leftPad(cols.apply(AirlineCol.DepTime), 4, "0");
        String arrTime = StringUtils.leftPad(cols.apply(AirlineCol.ArrTime), 4, "0");
        String origin = cols.apply(AirlineCol.Origin);
        String dest = cols.apply(AirlineCol.Dest);
        String distance = cols.apply(AirlineCol.Distance);
        String actualElapsedTime = cols.apply(AirlineCol.ActualElapsedTime);
        String CRSElapsedTime = cols.apply(AirlineCol.CRSElapsedTime);
        String depDelay = cols.apply(AirlineCol.DepDelay);
        String arrDelay = cols.apply(AirlineCol.ArrDelay);

        return new StringJoiner(",")
                .add(date)
                .add(dayOfWeek)
                .add(depTime)
                .add(arrTime)
                .add(origin)
                .add(dest)
                .add(distance)
                .add(actualElapsedTime)
                .add(CRSElapsedTime)
                .add(depDelay)
                .add(arrDelay)
                .toString();
    }


    // 不需要Reducer

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance();
        job.setJarByClass(SelectClause.class);


        // TextInputFormat为默认值，继承FileOutputFormat
        // TextInputFormat逐行读取数据，对压缩文件能透明地处理
        job.setInputFormatClass(TextInputFormat.class);
        // TextOutFormat默认输出未压缩的文本文件
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        // 不需要设置
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(SelectClauseMapper.class);
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

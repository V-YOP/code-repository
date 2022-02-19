
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.Util;

import java.io.IOException;

/**
 * Hadoop权威指南的第一个示例，输入数据为所谓的气温数据，每日三条，为特定格式的纯字符串    ，测试数据见URL，这数据肯定是假的。
 * @see <a href="https://github.com/tomwhite/hadoop-book/tree/master/input/ncdc/all">https://github.com/tomwhite/hadoop-book/tree/master/input/ncdc/all</a>
 */
public class MaxDayTemperature {
    // edit it!
    private static final String DATA_PATH = "file:///Users/builder/data/maxDayTemperature";

    private static final String INPUT_PATH = DATA_PATH + "/input";
    private static final String OUTPUT_PATH = DATA_PATH + "/output";


    // main的代码显然有一种模式，但为了保持"纯真"，这里不抽象了
    public static void main(String[] args) throws Exception {
        Util.deleteFile(OUTPUT_PATH);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Day Temperature");
        job.setJarByClass(MaxDayTemperature.class);
        job.setMapperClass(MaxDayTemperatureMapper.class);
        job.setCombinerClass(MaxDayTemperatureReducer.class);
        job.setReducerClass(MaxDayTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * 输入文件格式为0029227070999991902123120004+62167+030650FM-12+010299999V0200501N002119999999N0000001N9-01831+99999100111ADDGF100991999999999999999999<br/>
     * 通过substring(15, 23)获得日期，substring(88, 93)获得十倍的气温带正负号<br/>
     * Mapper的作用为将输入文件进行格式化，得到诸如&lt;19020203,35&gt;这样的KV
     */
    public static class MaxDayTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text outputK = new Text();
        private final IntWritable outputV = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String originalStr = value.toString();
            String date = originalStr.substring(15, 23);
            int temperature = Integer.parseInt(originalStr.substring(87, 93)) / 10;

            outputK.set(date);
            outputV.set(temperature);
            context.write(outputK, outputV);
        }
    }

    /**
     * 挑选最大值的Reducer，写法就如字面上的一样
     * reducer拿到的值形如&lt;19020304, [20, 10, -20]&gt;，找到V中的最大值
     */
    public static class MaxDayTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable res = new IntWritable();

        /**
         * 可惜这个Iterable没有reduce方法，只能自己包装了
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                int curr = value.get();
                if (curr > max)
                    max = curr;
            }

            res.set(max);
            context.write(key, res);
        }
    }
}

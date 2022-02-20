package phonedata;

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
 * 尚硅谷大数据课程的示例，涉及到自定义Bean的序列化和反序列化，目标是对一个手机访问数据表进行统计，获取每个手机号对总上行流量，下行流量，总流量<br />
 *
 * （按行）      ID     PHONE_NUMBER IP             WEBSITE(Optional) UP    DOWN    STATUS
 * 原数据    ：  1	    13736230513	 192.196.100.1	www.atguigu.com	  2481	24681	200
 * -----------
 *             KEY
 *             PHONE_NUMBER  UP     DOWN   SUM    （使用Flow类序列化）
 * Mapper后  ：13736230513   2481   24681  27162
 * -----------
 *             KEY
 *             PHONE_NUMBER  UP      DOWN     SUM （使用Flow类序列化）
 * Reducer后 ：13736230513   46681   104681   151362
 * -----------
 * 感觉写这种MapReduce就是在写脚本，写出来的Mapper，Reducer都是和输入强耦合的，难以重用
 */
public class PhoneDataStatistic {
    // edit it!
    private static final String DATA_PATH = "file:///Users/builder/data/phoneDataStatistic";

    private static final String INPUT_PATH = DATA_PATH + "/input";
    private static final String OUTPUT_PATH = DATA_PATH + "/output";

    // Mapper
    private static class PhoneDataMapper extends Mapper<LongWritable, Text, Text, Flow> {
        Text outputK = new Text();
        Flow outputV = new Flow();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] elems = value.toString().split("\\s+");

            String phoneNumber = elems[1];
//          根据元素的个数就能判断域名字段是否存在
//          int up;
//          int down;
//          if (elems.length == 7) {
//              up = Integer.parseInt(elems[4]);
//              down = Integer.parseInt(elems[5]);
//          } else {
//              up = Integer.parseInt(elems[3]);
//              down = Integer.parseInt(elems[4]);
//          }

//          也可以反着取
            int up = Integer.parseInt(elems[elems.length - 3]);
            int down = Integer.parseInt(elems[elems.length - 2]);
            outputK.set(phoneNumber);
            outputV.setUpFlow(up);
            outputV.setDownFlow(down);
            context.write(outputK, outputV);
        }
    }

    // Reducer
    private static class PhoneDataReducer extends Reducer<Text, Flow, Text, Flow> {
        Flow outputV = new Flow();
        @Override
        protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {
            long sumUp = 0;
            long sumDown = 0;
            for (Flow value : values) {
                sumUp += value.getUpFlow();
                sumDown += value.getDownFlow();
            }

            outputV.setUpFlow(sumUp);
            outputV.setDownFlow(sumDown);
            context.write(key, outputV);
        }
    }

    // Driver
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Util.deleteFile(OUTPUT_PATH);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "phone data statistic");
        job.setJarByClass(PhoneDataStatistic.class);
        job.setMapperClass(PhoneDataMapper.class);
        job.setCombinerClass(PhoneDataReducer.class);
        job.setReducerClass(PhoneDataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flow.class);
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

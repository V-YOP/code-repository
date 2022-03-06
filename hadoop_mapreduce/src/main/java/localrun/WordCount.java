package localrun;

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
import localrun.util.Util;

import java.io.IOException;

/**
 * 经典的word count，还用说什么呢？<br />
 * 官方示例使用StringTokenizer，但其注释显示split("\\s")能完全替代它的作用，因此这里使用split来切分字符串
 */
public class WordCount {
    // edit it!
    private static final String DATA_PATH = "file:///Users/builder/data/wordcount";

    private static final String INPUT_PATH = DATA_PATH + "/input";
    private static final String OUTPUT_PATH = DATA_PATH + "/output";


    private static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text outputK = new Text();
        private final IntWritable ONE = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            for (String s : value.toString().split("\\s")) {
                outputK.set(s);
                context.write(outputK, ONE);
            }
        }
    }

    private static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable res = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int[] sum = {0};
            values.forEach(val -> sum[0] = val.get() + sum[0]);

            res.set(sum[0]);
            context.write(key, res);
        }
    }


    public static void main(String[] args) throws Exception {
        Util.deleteFile(OUTPUT_PATH);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

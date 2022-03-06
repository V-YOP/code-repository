package localrun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import localrun.util.Util;

import java.io.IOException;

/**
 * 基于WordCount，测试多个小文件下使用WordCount会得到怎样的切片
 */
public class CombineSplit {
    // edit it!
    private static final String DATA_PATH = "file:///Users/builder/data/combineSplit";

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
        Job job = Job.getInstance(conf, "Combine Split");

        // !
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        job.setJarByClass(CombineSplit.class);
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

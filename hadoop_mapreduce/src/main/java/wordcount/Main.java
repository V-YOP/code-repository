package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.Util;

/**
 * 经典的word count，还用说什么呢？
 */
public class Main {
    // edit it!
    private static final String DATA_PATH = "file:///Users/builder/data/wordcount";

    private static final String INPUT_PATH = DATA_PATH + "/input";
    private static final String OUTPUT_PATH = DATA_PATH + "/output";

    public static void main(String[] args) throws Exception {

        Util.deleteFile(OUTPUT_PATH);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Main.class);
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

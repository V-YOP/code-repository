package mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 为了编写对比Hive的示例，更新一下WordCount，紧贴"最佳实践"
 */
public class WordCount {
    public static class NeoWordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final LongWritable ONE = new LongWritable(1);
        private final Text outputK = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            for (String s : value.toString().split(" ")) {
                outputK.set(s);
                context.write(outputK, ONE);
            }
        }
    }
    public static class NeoWordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable outputV = new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            outputV.set(sum);
            context.write(key, outputV);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        GenericOptionsParser parser = new GenericOptionsParser(args);
        Job job = Job.getInstance(parser.getConfiguration());
        args = parser.getRemainingArgs();

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);

        job.setMapperClass(NeoWordCountMapper.class);
        job.setReducerClass(NeoWordCountReducer.class);
        job.setCombinerClass(NeoWordCountReducer.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

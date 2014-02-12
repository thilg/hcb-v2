/**
 * Following sample is adopted from original wordcount sample from 
 * http://wiki.apache.org/hadoop/WordCount. 
 */
package chapter5.mbox;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Find number of replies sent by each person
 * 
 * @author Srinath Perera (hemapani@apache.org)
 * @author Thilina Gunarathne (thilina@apache.org)
 */
public class CountSentRepliesMapReduce extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CountSentRepliesMapReduce(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage:  <input_path> <output_path> <num_reduce_tasks>");
			System.exit(-1);
		}
		/* input parameters */
		String inputPath = args[0];
		String outputPath = args[1];
		int numReduce = 1;
		if (args.length == 3)
			numReduce = Integer.parseInt(args[2]);

		Job job = Job.getInstance(getConf(), "MLSendReplyProcessor");
		job.setJarByClass(CountReceivedRepliesMapReduce.class);
		job.setMapperClass(AMapper.class);
		job.setReducerClass(AReducer.class);
		job.setNumReduceTasks(numReduce);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(MBoxFileInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		int exitStatus = job.waitForCompletion(true) ? 0 : 1;
		return exitStatus;
	}

    public static class AMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("#");
            String from = tokens[0];
            String subject = tokens[1];
            String date = tokens[2];
            System.out.println(from + "=" + date);
            context.write(new Text(from), new Text(date));
        }
    }

    /**
     * <p>
     * Reduce function receives all the values that has the same key as the
     * input, and it output the key and the number of occurrences of the key as
     * the output.
     * </p>
     */
    public static class AReducer extends Reducer<Text, Text, Text, IntWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                sum = sum + 1;
            }
            context.write(key, new IntWritable(sum));
        }
    }
}

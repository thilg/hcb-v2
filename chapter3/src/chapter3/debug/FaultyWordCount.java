/**
 * Following sample is adopted from original wordcount sample from 
 * http://wiki.apache.org/hadoop/WordCount. 
 */
package chapter3.debug;

import java.io.IOException;
import java.util.StringTokenizer;

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
 * <p>
 * The word count sample counts the number of word occurrences within a set of
 * input documents using MapReduce. The code has three parts: mapper, reducer,
 * and the main program.
 * </p>
 * 
 * @author Srinath Perera (srinath@wso2.com)
 */
public class FaultyWordCount extends Configured implements Tool{

	/**
	 * <p>
	 * The mapper extends from the org.apache.hadoop.mapreduce.Mapper interface.
	 * When Hadoop runs, it receives each new line in the input files as an
	 * input to the mapper. The �map� function tokenize the line, and for each
	 * token (word) emits (word,1) as the output.
	 * </p>
	 */
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			if (value.toString().equals("**Injecting  an error**")) {
				throw new RuntimeException("Injected Failure");
			}
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	/**
	 * <p>
	 * Reduce function receives all the values that has the same key as the
	 * input, and it output the key and the number of occurrences of the key as
	 * the output.
	 * </p>
	 */
	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	/**
	 * <p>
	 * As input this program takes any text file. Create a folder called input
	 * in HDFS (or in local directory if you are running this locally)
	 * <ol>
	 * <li>You can compile the sample by ant from sample directory. To do this,
	 * you need to have Apache Ant installed in your system. Otherwise, you can
	 * use the complied jar included with the source code. hange directory to
	 * HADOOP_HOME, and copy the hadoop-cookbook.jar to the HADOOP_HOME. Then
	 * run the command > bin/hadoop jar hadoop-cookbook.jar chapter3.WordCount
	 * input output.</li>
	 * <li>As an optional step, copy the input directory to the top level of the
	 * IDE based project (eclipse project) that you created for samples. Now you
	 * can run the WordCount class directly from your IDE passing input output
	 * as arguments. This will run the sample same as before. Running MapReduce
	 * Jobs from IDE in this manner is very useful for debugging your MapReduce
	 * Jobs.</li>
	 * </ol>
	 * 
	 * @param args
	 * @throws Exception
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();		
		if (args.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "word count");
		job.getConfiguration().set("mapreduce.map.failures.maxpercent", "50");
		job.getConfiguration().set("mapreduce.reduce.failures.maxpercent",
				"50");
		job.setMaxMapAttempts(5);

		job.setJarByClass(FaultyWordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	   public static void main(String[] args) throws Exception {
	        int res = ToolRunner.run(new Configuration(), new FaultyWordCount(), args);
	        System.exit(res);
	    }
}

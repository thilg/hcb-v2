package chapter4.multipleoutputs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import chapter4.LogWritable;
import chapter4.inputformat.LogFileInputFormat;
import chapter4.inputformat.LogProcessorMap;

/**
 * HTTP server log processing sample for the Chapter 4 of Hadoop MapReduce
 * Cookbook. 
 * 
 * You can download the data for this sample from
 * http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html. 
 * You can find a description
 * of the structure of this data from
 * http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
 * 
 * @author Thilina Gunarathne
 * 
 */
public class LogProcessor extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new LogProcessor(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			System.err
					.println("Usage:  <input_path> <output_path> <num_reduce_tasks>");
			System.exit(-1);
		}

		/* input parameters */
		String inputPath = args[0];
		String outputPath = args[1];
		int numReduce = Integer.parseInt(args[2]);

		Job job = Job.getInstance(getConf(), "log-analysis");

		job.setJarByClass(LogProcessor.class);
		job.setMapperClass(LogProcessorMap.class);
		job.setReducerClass(LogProcessorReduce.class);
		job.setNumReduceTasks(numReduce);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LogWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(LogFileInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		MultipleOutputs.addNamedOutput(job, "responsesizes", TextOutputFormat.class,
				 Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "timestamps", TextOutputFormat.class,
				 Text.class, Text.class);
		
		int exitStatus = job.waitForCompletion(true) ? 0 : 1;

		return exitStatus;
	}
}
package chapter9;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Set;

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

import chapter9.amazondata.AmazonCustomer;
import chapter9.amazondata.AmazonCustomer.ItemData;
import chapter9.amazondata.AmazonInputDataFormat;

/**
 * 
 * @author Srinath Perera (hemapani@apache.org)
 */
public class MostFrequentUserFinder extends Configured implements Tool {
	public static SimpleDateFormat dateFormatter = new SimpleDateFormat(
			"EEEE dd MMM yyyy hh:mm:ss z");

	public static class AMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				List<AmazonCustomer> customerList = AmazonCustomer
						.parseAItemLine(value.toString());
				for (AmazonCustomer customer : customerList) {
					context.write(new Text(customer.customerID), new Text(
							customer.toString()));
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Error:" + e.getMessage());
			}
		}
	}

	public static class AReducer extends Reducer<Text, Text, IntWritable, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			AmazonCustomer customer = new AmazonCustomer();
			customer.customerID = key.toString();

			for (Text value : values) {
				Set<ItemData> itemsBrought = new AmazonCustomer(
						value.toString()).itemsBought;
				for (ItemData itemData : itemsBrought) {
					customer.itemsBought.add(itemData);
				}
			}
			if (customer.itemsBought.size() > 5) {
				context.write(new IntWritable(customer.itemsBought.size()),
						new Text(customer.toString()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new MostFrequentUserFinder(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			System.err
					.println("Usage:  <input_path> <output_path> <num_reduce_tasks>");
			System.exit(-1);
		}
		/* input parameters */
		String inputPath = args[0];
		String outputPath = args[1];
		int numReduce = 1;
		if (args.length == 3)
			numReduce = Integer.parseInt(args[2]);

		Job job = Job.getInstance(getConf(), "MostFrequentUserFinder");
		job.setJarByClass(MostFrequentUserFinder.class);
		job.setMapperClass(AMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		// Uncomment this to
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(AReducer.class);
		job.setInputFormatClass(AmazonInputDataFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return (job.waitForCompletion(true) ? 0 : 1);
	}
}

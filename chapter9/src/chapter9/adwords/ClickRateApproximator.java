package chapter9.adwords;

import java.io.IOException;
import java.util.List;

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

import chapter9.ClusterBasedRecommendation;
import chapter9.MostFrequentUserFinder;
import chapter9.amazondata.AmazonCustomer;
import chapter9.amazondata.AmazonCustomer.ItemData;
import chapter9.amazondata.AmazonInputDataFormat;

/**
 * This class approximates the click through rate for each keyword using the
 * keyword's sale's ranks
 * 
 * @author Srinath Perera (hemapani@apache.org)
 */
public class ClickRateApproximator extends Configured implements Tool {

	public static class AMapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				ItemData itemData = null;
				List<AmazonCustomer> customerList = AmazonCustomer
						.parseAItemLine(value.toString());
				if (customerList.size() == 0) {
					return;
				}
				for (AmazonCustomer customer : customerList) {
					itemData = customer.itemsBought.iterator().next();
					break;
				}

				if (itemData.title == null || itemData.salesrank == 0) {
					System.out.println("Igonred " + value.toString()
							+ "salesrank=" + itemData.salesrank);
					return;
				}
				String[] tokens = itemData.title.split("\\s");
				for (String token : tokens) {
					if (token.length() > 3) {
						context.write(new Text(token), new IntWritable(
								itemData.salesrank));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Error:" + e.getMessage());
			}
		}
	}

	public static class AReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			double clickrate = 0;
			// we approximate click rate by adding the 1/sales rank for
			// occurance of the word
			for (IntWritable val : values) {
				if (val.get() > 1) {
					clickrate = clickrate + 1000 / Math.log(val.get());
				} else {
					clickrate = clickrate + 1000;
				}
			}
			context.write(new Text("keyword:" + key.toString()),
					new IntWritable((int) clickrate));
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new ClusterBasedRecommendation(), args);
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

		Job job = Job.getInstance(getConf(), "ClickRateApproximator");
		job.setJarByClass(MostFrequentUserFinder.class);
		job.setMapperClass(AMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setReducerClass(AReducer.class);
		job.setInputFormatClass(AmazonInputDataFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return (job.waitForCompletion(true) ? 0 : 1);
	}
}

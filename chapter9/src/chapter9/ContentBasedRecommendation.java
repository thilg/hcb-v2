package chapter9;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
/*
* @author Srinath Perera (hemapani@apache.org)
*/
public class ContentBasedRecommendation extends Configured implements Tool {
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

		Job job = Job.getInstance(getConf(), "ContentBasedRecommendation");

		job.setJarByClass(ContentBasedRecommendation.class);
		job.setMapperClass(AMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(AReducer.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * We make recommendations for a user by looking at the items similar to the
	 * items that has brought by the user.
	 */

	public static class AMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			AmazonCustomer amazonCustomer = new AmazonCustomer(value.toString()
					.replaceAll("[0-9]+\\s+", ""));

			List<String> recommendations = new ArrayList<String>();
			for (ItemData itemData : amazonCustomer.itemsBought) {
				recommendations.addAll(itemData.similarItems);
			}

			for (ItemData itemData : amazonCustomer.itemsBought) {
				recommendations.remove(itemData.itemID);
			}

			ArrayList<String> finalRecommendations = new ArrayList<String>();
			for (int i = 0; i < Math.min(10, recommendations.size()); i++) {
				finalRecommendations.add(recommendations.get(i));
			}
			context.write(new Text(amazonCustomer.customerID), new Text(
					finalRecommendations.toString()));
		}
	}

	public static class AReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}
}

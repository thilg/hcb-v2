package chapter9;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

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
import chapter9.amazondata.AmazonCustomer.SortableItemData;
/*
* @author Srinath Perera (hemapani@apache.org)
*/
public class ClusterBasedRecommendation extends Configured implements Tool {
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

		Job job = Job.getInstance(getConf(), "ClusterBasedRecommendation");

		job.setJarByClass(ClusterBasedRecommendation.class);
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
	 * We make recommendations for each users by looking at the items brought by
	 * the other users in the same cluster as this user.
	 */
	public static class AMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			AmazonCustomer amazonCustomer = new AmazonCustomer(value.toString()
					.replaceAll("[0-9]+\\s+", ""));
			context.write(new Text(amazonCustomer.clusterID), new Text(
					amazonCustomer.toString()));
		}
	}

	public static class AReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<AmazonCustomer> customerList = new ArrayList<AmazonCustomer>();
			TreeSet<AmazonCustomer.SortableItemData> highestRated1000Items = new TreeSet<AmazonCustomer.SortableItemData>();
			for (Text value : values) {
				AmazonCustomer customer = new AmazonCustomer(value.toString());
				for (ItemData itemData : customer.itemsBought) {
					highestRated1000Items.add(customer.new SortableItemData(
							itemData));
					if (highestRated1000Items.size() > 1000) {
						highestRated1000Items.remove(highestRated1000Items
								.last());
					}
				}
				customerList.add(customer);
			}

			for (AmazonCustomer amazonCustomer : customerList) {
				List<ItemData> recemndationList = new ArrayList<AmazonCustomer.ItemData>();
				for (SortableItemData sortableItemData : highestRated1000Items) {
					if (!amazonCustomer.itemsBought
							.contains(sortableItemData.itemData)) {
						recemndationList.add(sortableItemData.itemData);
					}
				}
				ArrayList<ItemData> finalRecomendations = new ArrayList<ItemData>();
				for (int i = 0; i < 10; i++) {
					finalRecomendations.add(recemndationList.get(i));
				}

				context.write(new Text(amazonCustomer.customerID), new Text(
						finalRecomendations.toString()));
			}
		}
	}
}

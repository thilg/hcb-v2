package chapter5.mbox;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TreeMap;

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

/**
 * Find number of owner and replies received by each thread
 * 
 * Sample data can be downloaded from http://tomcat.apache.org/mail/dev/
 * 
 * @author Srinath Perera (hemapani@apache.org)
 * @author Thilina Gunarathne (thilina@apache.org)
 */
public class CountReceivedRepliesMapReduce extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CountReceivedRepliesMapReduce(), args);
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

		Job job = Job.getInstance(getConf(), "MLReceiveReplyProcessor");
		job.setJarByClass(CountReceivedRepliesMapReduce.class);
		job.setMapperClass(AMapper.class);
		job.setReducerClass(AReducer.class);
		job.setNumReduceTasks(numReduce);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
			String date = tokens[2].replaceAll(",", "");
			subject = subject.replaceAll("Re:", "");
			context.write(new Text(subject), new Text(date + "#" + from));
		}
	}

	public static class AReducer extends Reducer<Text, Text, Text, Text> {
		public static SimpleDateFormat dateFormatter = new SimpleDateFormat("EEEE dd MMM yyyy hh:mm:ss z");

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try {
				TreeMap<Long, String> replyData = new TreeMap<Long, String>();
				for (Text val : values) {
					String[] tokens = val.toString().split("#");
					if (tokens.length != 2) {
						throw new IOException("Unexpected token " + val.toString());
					}
					String from = tokens[1];
					Date date = dateFormatter.parse(tokens[0]);
					replyData.put(date.getTime(), from);
				}
				String owner = replyData.get(replyData.firstKey());
				int replyCount = replyData.size();
				int selfReplies = 0;
				for (String from : replyData.values()) {
					if (owner.equals(from)) {
						selfReplies++;
					}
				}
				replyCount = replyCount - selfReplies;

				context.write(new Text(owner), new Text(replyCount + "#" + selfReplies));
			} catch (Exception e) {
				System.out.println("ERROR:" + e.getMessage());
				return;
				// throw new IOException(e);
			}
		}
	}
}

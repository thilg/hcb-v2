package chapter5.mbox;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import chapter5.mbox.CountSentRepliesMapReduce.AMapper;
import chapter5.mbox.CountSentRepliesMapReduce.AReducer;

/**
 * Joined the replies sent and replies received by each sender
 * 
 * @author Srinath Perera (hemapani@apache.org)
 * @author Thilina Gunarathne (thilina@apache.org)
 */
public class JoinSentReceivedReplies extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new JoinSentReceivedReplies(), args);
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

		Job job = Job.getInstance(getConf(), "MLJoinSendReceiveReplies");
		job.setJarByClass(JoinSentReceivedReplies.class);
		job.setMapperClass(AMapper.class);
		job.setReducerClass(AReducer.class);
		job.setNumReduceTasks(numReduce);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		int exitStatus = job.waitForCompletion(true) ? 0 : 1;
		return exitStatus;
	}

    public static class AMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s");
            String from = tokens[0];
            String replyData = tokens[1];
            context.write(new Text(from), new Text(replyData));
        }
    }

    public static class AReducer extends Reducer<Text, Text, IntWritable, IntWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer buf = new StringBuffer("[");

            try {
                int sendReplyCount = 0;
                int receiveReplyCount = 0;
                for (Text val : values) {
                    String strVal = val.toString();
                    buf.append(strVal).append(",");
                    if (strVal.contains("#")) {
                        String[] tokens = strVal.split("#");
                        int repliesOnThisThread = Integer.parseInt(tokens[0]);
                        int selfRepliesOnThisThread = Integer.parseInt(tokens[1]);
                        receiveReplyCount = receiveReplyCount + repliesOnThisThread;
                        sendReplyCount = sendReplyCount - selfRepliesOnThisThread;
                    } else {
                        sendReplyCount = sendReplyCount + Integer.parseInt(strVal);
                    }
                }
                context.write(new IntWritable(sendReplyCount), new IntWritable(receiveReplyCount));
                buf.append("]");
            } catch (NumberFormatException e) {
                System.out.println("ERROR " + e.getMessage() + " " + buf);
            }
        }
    }
}

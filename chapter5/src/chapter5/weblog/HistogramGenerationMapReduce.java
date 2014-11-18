package chapter5.weblog;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * Finds the number of hits received by each URL
 * 
 * @author Srinath Perera (hemapani@apache.org)
 * @author Thilina Gunarathne (thilina@apache.org)
 */

public class HistogramGenerationMapReduce extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new HistogramGenerationMapReduce(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage:  <input_path> <output_path> <num_reduce_tasks>");
			System.exit(-1);
		}
		/* input parameters */
		String inputPath = args[0];
		String outputPath = args[1];
		int numReduce = 1;
		if (args.length == 3)
			numReduce = Integer.parseInt(args[2]);

		Job job = Job.getInstance(getConf(), "WeblogTimeOfDayHistogramCreator");
		job.setJarByClass(HistogramGenerationMapReduce.class);
		job.setMapperClass(AMapper.class);
		job.setReducerClass(AReducer.class);
		job.setNumReduceTasks(numReduce);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		int exitStatus = job.waitForCompletion(true) ? 0 : 1;
		return exitStatus;
	}

	public static class AMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		public static final Pattern httplogPattern = Pattern
				.compile("([^\\s]+) - - \\[(.+)\\] \"([^\\s]+) (/[^\\s]*) HTTP/[^\\s]+\" [^\\s]+ ([0-9]+)");
		public static SimpleDateFormat dateFormatter = new SimpleDateFormat("dd/MMMMM/yyyy:hh:mm:ss z");

		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				Matcher matcher = httplogPattern.matcher(value.toString());
				if (matcher.matches()) {
					String timeAsStr = matcher.group(2);
					Date time = dateFormatter.parse(timeAsStr);
					// creates a new calendar instance
					Calendar calendar = GregorianCalendar.getInstance();
					// assigns calendar to given date
					calendar.setTime(time);
					// gets hour in 24h format
					int hour = calendar.get(Calendar.HOUR_OF_DAY);
					context.write(new IntWritable(hour), one);
				}
			} catch (ParseException e) {
				// Ignore any misformatted entries
				e.printStackTrace();
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
	public static class AReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}

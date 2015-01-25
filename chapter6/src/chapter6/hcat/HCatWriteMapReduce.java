package chapter6.hcat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

public class HCatWriteMapReduce extends Configured implements Tool {

	public static class UserReadMapper extends
			Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable> {

		IntWritable ONE = new IntWritable(1);

		@Override
		public void map(
				WritableComparable key,
				HCatRecord value,
				Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {

			HCatSchema schema = HCatBaseInputFormat.getTableSchema(context
					.getConfiguration());
			// To avoid the "null" values in the age field of the User table
			Object ageObject = value.get("age", schema);
			if (ageObject instanceof Integer) {
				int age = ((Integer) ageObject).intValue();
				// emit age and one for count
				context.write(new IntWritable(age), ONE);
			}
		}
	}

	public static class Reduce extends
			Reducer<IntWritable, IntWritable, WritableComparable, HCatRecord> {

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			if (key.get() < 34 & key.get() > 18) {
				int count = 0;
				for (IntWritable val : values) {
					count += val.get();
				}
				HCatRecord record = new DefaultHCatRecord(2);
				record.set(0, key.get());
				record.set(1, count);
				context.write(null, record);
			}
		}
	}

	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			System.err
					.println("Usage:  <dbname> <in-tablename> <out-tablename>");
			System.exit(-1);
		}
		/* input parameters */
		String dbName = args[0];
		String inTableName = args[1];
		String outTableName = args[2];

		Job job = Job.getInstance(getConf(), "HCatMapReduceSample");
		job.setJarByClass(HCatWriteMapReduce.class);
		job.setMapperClass(UserReadMapper.class);
		job.setReducerClass(Reduce.class);

		// Set HCatalog as the InputFormat
		job.setInputFormatClass(HCatInputFormat.class);
		HCatInputFormat.setInput(job, dbName, inTableName);

		// Mapper emits a string as key and an integer as value
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Ignore the key for the reducer output; emitting an HCatalog record as
		// value
		job.setOutputKeyClass(WritableComparable.class);
		job.setOutputValueClass(DefaultHCatRecord.class);
		job.setOutputFormatClass(HCatOutputFormat.class);

		HCatOutputFormat.setOutput(job,
				OutputJobInfo.create(dbName, outTableName, null));
		HCatSchema schema = HCatOutputFormat.getTableSchema(job
				.getConfiguration());
		HCatOutputFormat.setSchema(job, schema);

		int exitStatus = job.waitForCompletion(true) ? 0 : 1;
		return exitStatus;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new HCatWriteMapReduce(),
				args);
		System.exit(res);
	}
}

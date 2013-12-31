package chapter4.jobdependency;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * HTTP server log processing sample for the Chapter 4 of Hadoop MapReduce
 * Cookbook. 
 * 
 * @author Thilina Gunarathne
 */
public class RegexMapper extends Mapper<Object, Text, Text, NullWritable> {
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Add your regex filtering code here
		context.write(value, NullWritable.get());
	}
}

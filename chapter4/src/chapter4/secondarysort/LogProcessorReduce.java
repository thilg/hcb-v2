package chapter4.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import chapter4.LogWritable;

/**
 * HTTP server log processing sample for the Chapter 4 of Hadoop MapReduce
 * Cookbook.
 * 
 * @author Thilina Gunarathne
 */
public class LogProcessorReduce extends
		Reducer<SecondarySortWritable, LogWritable, Text, IntWritable> {
	private Text visitorAddress = new Text();

	public void reduce(SecondarySortWritable key, Iterable<LogWritable> values,
			Context context) throws IOException, InterruptedException {
		visitorAddress.set(key.getVisitorAddress());
		for (LogWritable val : values) {			
			context.write(visitorAddress, val.getResponseSize());
		}
	}
}

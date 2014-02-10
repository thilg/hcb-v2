package chapter4.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import chapter4.LogWritable;

/**
 * HTTP server log processing sample for the Chapter 4 of Hadoop MapReduce
 * Cookbook. 
 * 
 * @author Thilina Gunarathne
 */
public class LogProcessorMap extends Mapper<Object, LogWritable, Text, LogWritable > {
	
	
	public void map(Object key, LogWritable value, Context context)
			throws IOException, InterruptedException {

		context.write(value.getUserIP(),value);
	}
	
}

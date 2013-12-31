package chapter4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * HTTP server log processing sample for the Chapter 4 of Hadoop MapReduce
 * Cookbook. 
 * 
 * @author Thilina Gunarathne
 */
public class LogProcessorMap extends Mapper<Object, LogWritable, Text, IntWritable > {

	public static enum LOG_PROCESSOR_COUNTER {
		  BAD_RECORDS,
		  PROCCESSED_RECORDS
		};

	
	public void map(Object key, LogWritable value, Context context)
			throws IOException, InterruptedException {		
		//context.getCounter(LOG_PROCESSOR_COUNTER.BAD_RECORDS).increment(1);

		context.write(value.getUserIP(),value.getResponseSize());
	}
}

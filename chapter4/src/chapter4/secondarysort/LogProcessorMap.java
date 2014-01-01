package chapter4.secondarysort;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import chapter4.LogWritable;

/**
 * HTTP server log processing sample for the Chapter 4 of Hadoop MapReduce
 * Cookbook. 
 * 
 * @author Thilina Gunarathne
 */
public class LogProcessorMap extends Mapper<Object, LogWritable, SecondarySortWritable, LogWritable > {
	private SecondarySortWritable outKey =  new SecondarySortWritable();
	
	public void map(Object key, LogWritable value, Context context)
			throws IOException, InterruptedException {		
		outKey.set(value.getUserIP().toString(), value.getResponseSize().get());
		context.write(outKey,value);
	}
}

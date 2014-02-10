package chapter4;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * HTTP server log processing sample for the Chapter 4 of Hadoop MapReduce
 * Cookbook. 
 * 
 * @author Thilina Gunarathne
 */
public class LogProcessorMap extends Mapper<LongWritable, Text, Text, LogWritable > {
	LogWritable outValue = new LogWritable();
	Text outKey = new Text();
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String logEntryPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)";

		Pattern p = Pattern.compile(logEntryPattern);
		Matcher matcher = p.matcher(value.toString());
		if (!matcher.matches()) {
			System.err.println("Bad Record : "+value);
			return;
		}
		
		String userIP = matcher.group(1);
		String timestamp = matcher.group(4);
		String request = matcher.group(5);
		int status = Integer.parseInt(matcher.group(6));
		int bytes = Integer.parseInt(matcher.group(7));
		
		outKey.set(userIP);
		outValue.set(userIP, timestamp, request,
				bytes,status);
		context.write(outKey,outValue);
	}
	
}

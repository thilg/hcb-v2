package chapter3;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class WordCountWithTools extends Configured implements Tool {
	  /**
	   * <p>
	   * The mapper extends from the org.apache.hadoop.mapreduce.Mapper interface. When Hadoop runs, 
	   * it receives each new line in the input files as an input to the mapper. The "map" function 
	   * tokenize the line, and for each token (word) emits (word,1) as the output.  </p>
	   */
	  public static class TokenizerMapper 
	       extends Mapper<Object, Text, Text, IntWritable>{
	    
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	      
	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens()) {
	        word.set(itr.nextToken());
	        context.write(word, one);
	      }
	    }
	  }
	  
	  /**
	   * <p>Reduce function receives all the values that has the same key as the input, and it output the key 
	   * and the number of occurrences of the key as the output.</p>  
	   */
	  public static class IntSumReducer 
	       extends Reducer<Text,IntWritable,Text,IntWritable> {
	    private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }
	  }
	  
	  
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("chapter3.WordCountWithTools <inDir> <outDir>");
            ToolRunner.printGenericCommandUsage(System.out);
            System.out.println("");
            return -1;
        }
        String inputPath = args[0];
        String outPath = args[1];

        Job job = prepareJob(inputPath, outPath, getConf());
        job.waitForCompletion(true);

        return 0;
    }

	public Job prepareJob(String inputPath, String outPath,Configuration conf)
			throws IOException {
		Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountWithTools.class);
        job.setMapperClass(TokenizerMapper.class);
        // Uncomment this to
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		return job;
	}

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountWithTools(), args);
        System.exit(res);
    }
}

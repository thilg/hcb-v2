package chapter3;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class WordCountWithToolsTest {

	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text,IntWritable,Text,IntWritable> reduceDriver;
	MapReduceDriver<Object, Text, Text, IntWritable, Text,IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		WordCountWithTools.TokenizerMapper mapper = new WordCountWithTools.TokenizerMapper();
		WordCountWithTools.IntSumReducer reducer = new WordCountWithTools.IntSumReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void testWordCountMapper() throws IOException {
		IntWritable inKey = new IntWritable(0);
		mapDriver.withInput(inKey, new Text("Test Quick"));
		mapDriver.withInput(inKey, new Text("Test Quick"));
		
		mapDriver.withOutput(new Text(
				"Test"),new IntWritable(1));
		mapDriver.withOutput(new Text(
				"Quick"),new IntWritable(1));
		mapDriver.withOutput(new Text(
				"Test"),new IntWritable(1));
		mapDriver.withOutput(new Text(
				"Quick"),new IntWritable(1));
		mapDriver.runTest();
		
	}
	
	@Test
	public void testWordCountReduce() throws IOException {
		
		ArrayList<IntWritable> reduceInList = new ArrayList<IntWritable>();
		reduceInList.add(new IntWritable(1));
		reduceInList.add(new IntWritable(2));

		reduceDriver.withInput(new Text("Quick"), reduceInList);
		reduceDriver.withInput(new Text("Test"), reduceInList);
		
		ArrayList<Pair<Text, IntWritable>> reduceOutList = new ArrayList<Pair<Text,IntWritable>>();
		reduceOutList.add(new Pair<Text, IntWritable>(new Text(
				"Quick"),new IntWritable(3)));
		reduceOutList.add(new Pair<Text, IntWritable>(new Text(
				"Test"),new IntWritable(3)));
		
		reduceDriver.withAllOutput(reduceOutList);
		reduceDriver.runTest();
	}
	
	@Test
	public void testWordCountMapReduce() throws IOException {
		
		IntWritable inKey = new IntWritable(0);
		mapReduceDriver.withInput(inKey, new Text("Test Quick"));
		mapReduceDriver.withInput(inKey, new Text("Test Quick"));
		
		ArrayList<Pair<Text, IntWritable>> reduceOutList = new ArrayList<Pair<Text,IntWritable>>();
		reduceOutList.add(new Pair<Text, IntWritable>(new Text(
				"Quick"),new IntWritable(2)));
		reduceOutList.add(new Pair<Text, IntWritable>(new Text(
				"Test"),new IntWritable(2)));

		mapReduceDriver.withAllOutput(reduceOutList);
		mapReduceDriver.runTest();
	}
}

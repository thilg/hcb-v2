package chapter9.amazondata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author Srinath Perera (hemapani@apache.org)
 * @author Thilina Gunarathne
 */
public class AmazonInputDataFormat extends FileInputFormat<Text, Text>{
    private AmazonDataReader reader = null; 
        
   
    @Override
    public RecordReader<Text, Text> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext attempt) throws IOException,
            InterruptedException {
        reader = new AmazonDataReader();
        reader.initialize(inputSplit, attempt);
        return reader;
    }

}
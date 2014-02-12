package chapter5.mbox;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Used to read Mbox files 
 * @author Srinath Perera (hemapani@apache.org)
 * @author Thilina Gunarathne (thilina@apache.org)
 */
public class MBoxFileInputFormat extends FileInputFormat<Text, Text>{
    private MBoxFileReader boxFileReader = null; 
   
    @Override
    public RecordReader<Text, Text> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext attempt) throws IOException,
            InterruptedException {
        boxFileReader = new MBoxFileReader();
        boxFileReader.initialize(inputSplit, attempt);
        return boxFileReader;
    }

}
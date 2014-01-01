package chapter4.secondarysort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * HTTP server log processing sample for the Chapter 4 of Hadoop MapReduce
 * Cookbook. 
 * Partitioner for secondary sorting.
 * 
 * @author Thilina Gunarathne
 */
public class SingleFieldPartitioner extends
		Partitioner<SecondarySortWritable, Writable> {
	@Override
	public int getPartition(SecondarySortWritable key, Writable value,
			int numPartitions) {
		return (int)(key.getVisitorAddress().hashCode() % numPartitions);
	}
}
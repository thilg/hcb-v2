package chapter4.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * HTTP server log processing sample for the Chapter 4 of Hadoop MapReduce
 * Cookbook. 
 * Grouping comparator for secondary sorting.
 * @author Thilina Gunarathne
 */
public class GroupingComparator extends WritableComparator {

	public GroupingComparator() {
		super(SecondarySortWritable.class, true);
	}

	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {
		SecondarySortWritable firstKey = (SecondarySortWritable) o1;
		SecondarySortWritable secondKey = (SecondarySortWritable) o2;
		return (firstKey.getVisitorAddress()).compareTo(secondKey
				.getVisitorAddress());
	}
}
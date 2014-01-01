package chapter4.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * HTTP server log processing sample for the Chapter 4 of Hadoop MapReduce
 * Cookbook. 
 * Writable for secondary sorting key.
 * 
 * @author Thilina Gunarathne
 */
public class SecondarySortWritable implements
WritableComparable<SecondarySortWritable> {

	private String visitorAddress;
	
	private int responseSize;

	public SecondarySortWritable() {
		super();
	}
	
	public String getVisitorAddress() {
		return visitorAddress;
	}

	public int getResponseSize() {
		return responseSize;
	}

	public void set(String visitorAddress, int responseSize) {
		this.visitorAddress = visitorAddress;
		this.responseSize = responseSize;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(visitorAddress);
		out.writeInt(responseSize);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		visitorAddress = in.readUTF();
		responseSize = in.readInt();
	}

	@Override
	public boolean equals(Object right) {
		if (right instanceof SecondarySortWritable) {
			SecondarySortWritable r = (SecondarySortWritable) right;
			return (r.visitorAddress.equals(visitorAddress) && (r.responseSize == responseSize));
		} else {
			return false;
		}
	}

	/** A Comparator that compares serialized key. */
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(SecondarySortWritable.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes( b1, s1, l1,b2, s2, l2);
		}
	}

	static { // register this comparator
		WritableComparator
				.define(SecondarySortWritable.class, new Comparator());
	}

	/*
	 * Sort order is visitorAddress, responseSize
	 */
	@Override
	public int compareTo(SecondarySortWritable o) {
		if (visitorAddress.equals(visitorAddress)) {
			return responseSize < o.responseSize ? -1 : 1;
		} else {
			return visitorAddress.compareTo(o.visitorAddress);
		} 
	}
}
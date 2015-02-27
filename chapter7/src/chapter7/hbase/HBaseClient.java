package chapter7.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Following shows how to connect to HBase from java.
 * 
 * @author srinath
 * @author tgunarathne
 */
public class HBaseClient {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		String hbaseZNode = "/hbase-unsecure";
		String zQuorum = "localhost";

		if (args.length == 1) {
			zQuorum = args[0];
		} else if (args.length == 2) {
			zQuorum = args[0];
			hbaseZNode = args[1];
		} else {
			System.out
					.println("Usage: gradle executeHBaseClient "
							+ "-Dexec.args=\"<One or more servers from Zookeeper Quorum> (root znode for HBase)\"\n");
			System.out.println("Trying to proceed using the default values...\n\n");
		}

		Configuration conf = HBaseConfiguration.create();
		conf.clear();

		conf.set("hbase.zookeeper.quorum", zQuorum);
		// conf.set("hbase.zookeeper.property.clientPort","2181");
		conf.set("zookeeper.znode.parent", hbaseZNode);
		HBaseAdmin.checkHBaseAvailable(conf);
		System.out.println("Connected to HBase");
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (!admin.tableExists("test")){
			System.out.println("Please create the 'test' table in HBase.");
			System.exit(-1);
		}
		admin.close();

		HTable table = new HTable(conf, "test");
		

		// putting data to HBase 
		Put put = new Put("row1".getBytes());
		put.add("cf".getBytes(), "b".getBytes(), "val2".getBytes());
		table.put(put);

		// reading data from HBase
		Scan s = new Scan();
		s.addFamily(Bytes.toBytes("cf"));
		ResultScanner results = table.getScanner(s);

		try {
			for (Result result : results) {
				for (Cell keyValue : result.listCells()) {
					System.out.println(new String(keyValue.getFamilyArray()) + " "
							+ new String(keyValue.getQualifierArray()) + "="
							+ new String(keyValue.getValueArray()));
				}
			}
		} finally {
			results.close();
		}
		
		table.close();
	}

}

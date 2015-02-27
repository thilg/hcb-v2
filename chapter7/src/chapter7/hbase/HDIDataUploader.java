package chapter7.hbase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import utils.CSVLineParser;

/**
 * This class read the data file and upload the data to HBase
 * 
 * @author srinath
 * @author tgunarathne
 *
 */
public class HDIDataUploader {

	private static final String TABLE_NAME = "HDI";

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		String hbaseZNode = "/hbase-unsecure";
		String zQuorum = "localhost";
		String fileName = "resources/hdi-data.csv";

		if (args.length == 2) {
			zQuorum = args[0];
			fileName = args[1];
		} else if (args.length == 3) {
			zQuorum = args[0];
			fileName = args[1];
			hbaseZNode = args[2];
		} else {
			System.out
				.println("Usage: gradle executeHDIDataUpload "
				+ "-Dexec.args=\"<One or more servers from Zookeeper Quorum> <hdi-data file> (root znode for HBase)\"\n");
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
		if (!admin.tableExists(TABLE_NAME)){
			System.out.println("Please create the '"+TABLE_NAME+"' table in HBase.");
			System.exit(-1);
		}
		admin.close();

		HTable table = new HTable(conf, TABLE_NAME);

		BufferedReader reader = new BufferedReader(new FileReader(fileName));

		try {
			String line = null;
			// skip first line
			reader.readLine();
			while ((line = reader.readLine()) != null) {
				try {
					String[] tokens = CSVLineParser.tokenizeCSV(line).toArray(
							new String[0]);
					String country = tokens[1];
					double lifeExpectancy = Double.parseDouble(tokens[3]
							.replaceAll(",", ""));
					double meanYearsOfSchooling = Double.parseDouble(tokens[4]
							.replaceAll(",", ""));
					double gnip = Double.parseDouble(tokens[6].replaceAll(",",
							""));

					Put put = new Put(Bytes.toBytes(country));
					put.add("ByCountry".getBytes(),
							Bytes.toBytes("lifeExpectancy"),
							Bytes.toBytes(lifeExpectancy));
					put.add("ByCountry".getBytes(),
							Bytes.toBytes("meanYearsOfSchooling"),
							Bytes.toBytes(meanYearsOfSchooling));
					put.add("ByCountry".getBytes(), Bytes.toBytes("gnip"),
							Bytes.toBytes(gnip));
					table.put(put);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("Error processing " + line
							+ " caused by " + e.getMessage());
				}
			}
		} catch (IOException e) {
			try {
				reader.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}

		table.close();
	}

}

package chapter7.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Following shows how to connect to HBase from java. 
 * @author srinath
 *
 */
public class HBaseClient {
public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM                     = "hbase.zookeeper.quorum";
public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT                 = "hbase.zookeeper.property.clientPort";
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception{

        
        //content to HBase
        Configuration conf = HBaseConfiguration.create();
String hbaseZookeeperQuorum="lyuba00";
String hbaseZookeeperClientPort="10000";
conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hbaseZookeeperQuorum);
conf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, hbaseZookeeperClientPort);

//        conf.set("hbase.master","lyuba00:60000");
          System.out.println("1");
      
        Configuration config = HBaseConfiguration.create();
        HTable table = new HTable(config, "test");
        System.out.println("2");
        
        //put data
/*        Put put = new Put("row1".getBytes());
        put.add("cf".getBytes(), "b".getBytes(), "val2".getBytes());
        table.put(put);
                System.out.println("3");
*/
        //read data
        Scan s = new Scan();
        s.addFamily(Bytes.toBytes("cf")); 
        ResultScanner results = table.getScanner(s);
               System.out.println("4");

        try {
            for(Result result: results){
                KeyValue[] keyValuePairs = result.raw(); 
                System.out.println(new String(result.getRow()));
                for(KeyValue keyValue: keyValuePairs){
                    System.out.println( new String(keyValue.getFamily()) + " "+ new String(keyValue.getQualifier()) + "=" + new String(keyValue.getValue()));
                }
            }
        } finally{
            results.close();
        }
        
    }

}

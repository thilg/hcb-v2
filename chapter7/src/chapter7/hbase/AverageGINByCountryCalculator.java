package chapter7.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Calculate the average of Gross National Income (GNI) per capita by country.
 * Dataset can be found from http://hdr.undp.org/en/statistics/data/.
 */

public class AverageGINByCountryCalculator  extends Configured implements Tool {

    static class Mapper extends TableMapper<ImmutableBytesWritable, DoubleWritable> {

        private int numRecords = 0;

        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
            byte[] results = values.getValue("ByCountry".getBytes(), "gnip".getBytes());

            // extract userKey from the compositeKey (userId + counter)
            ImmutableBytesWritable userKey = new ImmutableBytesWritable("ginp".getBytes());
            try {
                context.write(userKey, new DoubleWritable(Bytes.toDouble(results)));
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            numRecords++;
            if ((numRecords % 50) == 0) {
                context.setStatus("mapper processed " + numRecords + " records so far");
            }
        }
    }

    public static class Reducer extends TableReducer<ImmutableBytesWritable, DoubleWritable, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            Put put = new Put(key.get());
            put.add(Bytes.toBytes("data"), Bytes.toBytes("average"), Bytes.toBytes(sum / count));
            System.out.println("Processed " + count + " values and avergae =" + sum / count);
            context.write(key, put);
        }
    }

    public int run(String[] args) throws Exception {
		String hbaseZNode = "/hbase-unsecure";
		String zQuorum = "localhost";

		if (args.length == 1) {
			zQuorum = args[0];
		} else if (args.length == 2) {
			zQuorum = args[0];
			hbaseZNode = args[1];
		} else {
			System.out
					.println("Usage: hadoop jar build/libs/hcb-c7-samples-uber.jar chapter7.hbase.AverageGINByCountryCalculator "
							+ " <One or more servers from Zookeeper Quorum> (root znode for HBase)\n");
			System.out.println("Trying to proceed using the default values...\n\n");
		}

		Configuration conf = HBaseConfiguration.create();
		conf.clear();

		conf.set("hbase.zookeeper.quorum", zQuorum);
		// conf.set("hbase.zookeeper.property.clientPort","2181");
		conf.set("zookeeper.znode.parent", hbaseZNode);
		HBaseAdmin.checkHBaseAvailable(conf);
		System.out.println("Connected to HBase");
   
        Job job = Job.getInstance(conf,"AverageGINByCountryCalcualtor");
        job.setJarByClass(AverageGINByCountryCalculator.class);
        Scan scan = new Scan();
        scan.addFamily("ByCountry".getBytes());
        scan.setFilter(new FirstKeyOnlyFilter());
        TableMapReduceUtil.initTableMapperJob("HDI", scan, Mapper.class, ImmutableBytesWritable.class,
                DoubleWritable.class, job);
        TableMapReduceUtil.initTableReducerJob("HDIResult", Reducer.class, job);
        return (job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AverageGINByCountryCalculator(), args);
        System.exit(res);
    }
}

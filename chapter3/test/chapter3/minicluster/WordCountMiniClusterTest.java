package chapter3.minicluster;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.mapreduce.Job;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import chapter3.WordCountWithTools;

/**
 * Basic testing for the MiniMRClientCluster. This test shows an example class
 * that can be used in MR1 or MR2, without any change to the test. The test will
 * use MiniMRYarnCluster in MR2, and MiniMRCluster in MR1.
 */
public class WordCountMiniClusterTest {
	private static MiniMRClientCluster mrCluster;

	private class InternalClass {
	}

	@BeforeClass
	public static void setup() throws IOException {
		// create the mini cluster to be used for the tests
		mrCluster = MiniMRClientClusterFactory.create(InternalClass.class, 1,
				new Configuration());
	}

	@AfterClass
	public static void cleanup() throws IOException {
		// stopping the mini cluster
		mrCluster.stop();
	}

	@Test
	public void testSimpleMSISDNSum() throws Exception {
		String outDirString = "build/word-count-test";
		String testResDir = "test-resources";
		String testInput = testResDir + "/wc-input.txt";

		// clean output dir
		File outDir = new File(outDirString);
		FileUtils.deleteDirectory(outDir);

		Job job = (new WordCountWithTools()).prepareJob(testInput,
				outDirString, mrCluster.getConfig());
		assertTrue(job.waitForCompletion(true));
		// validateCounters(job.getCounters(), 5, 25, 5, 5);
	}
}

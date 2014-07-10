package chapter6.udf;

import java.net.MalformedURLException;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ExtractFileFromURLTest {

	@Test
	public void test() throws MalformedURLException {
		ExtractFilenameFromURL example = new ExtractFilenameFromURL();
		Assert.assertEquals("0060973129.01.THUMBZZZ.jpg",
				(example.evaluate
						(new Text("http://images.amazon.com/images/P/0060973129.01.THUMBZZZ.jpg?key=value"))
						.toString()));
	}
}

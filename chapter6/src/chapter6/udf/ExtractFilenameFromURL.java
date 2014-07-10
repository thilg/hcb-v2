package chapter6.udf;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = true)
@Description(
		name = "filename_from_url", 
		value = "Extracts and return the filename part of a URL.", 
		extended = "Extracts and return the filename part of a URL. "
				+ "filename_from_url('http://test.org/temp/test.jpg?key=value') returns 'test.jpg'."
)

public class ExtractFilenameFromURL extends UDF {
	
	public Text evaluate(Text input) throws MalformedURLException {
		URL url = new URL(input.toString());
		Text fileNameText = new Text(FilenameUtils.getName(url.getPath()));
		return fileNameText;
	}
}

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InListGenerationReducer implements
		Reducer<Text, Text, Text, Text> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		StringBuffer sb = new StringBuffer();
		while (values.hasNext()) {
			sb.append(values.next().toString());
			if (values.hasNext()) {
				sb.append(",");
			}
		}
		output.collect(key, new Text(sb.toString()));
	}
}

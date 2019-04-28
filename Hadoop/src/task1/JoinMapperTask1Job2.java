package task1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapperTask1Job2 extends Mapper<Object, Text, Text, Text> {
	
	@Override
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {

		String[] dataArray = value.toString().split("\t");
		if (dataArray.length >= 2) {
			context.write(new Text(dataArray[0]), new Text(dataArray[1]));
		}
	}

}

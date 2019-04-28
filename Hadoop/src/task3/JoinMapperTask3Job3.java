package task3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class JoinMapperTask3Job3 extends Mapper<Object, Text, Text, Text> {
	
	@Override
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {

		String[] temp = value.toString().split("\t");
		context.write(new Text(temp[0]), new Text(temp[1]));
	}

}

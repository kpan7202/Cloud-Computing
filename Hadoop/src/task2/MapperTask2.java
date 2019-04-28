package task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperTask2 extends Mapper<Object, Text, IntWritable, Text> {
	
	@Override
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {

		String[] dataArray = value.toString().split("\t");
		if (dataArray.length >= 2) {
			int count = Integer.parseInt(dataArray[1]);
			context.write(new IntWritable(count), new Text(dataArray[0]));
		}
	}

}

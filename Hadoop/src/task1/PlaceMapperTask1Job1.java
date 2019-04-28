package task1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PlaceMapperTask1Job1 extends  Mapper<Object, Text, TextIntPair, Text> {
	
	@Override
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
		
		String[] dataArray = value.toString().split("\t");
		if (dataArray.length >= 6) {
			if (dataArray[5].equals("7")) {
				context.write(new TextIntPair(dataArray[0],0), new Text(dataArray[4]));
			}
			else if (dataArray[5].equals("22")) {
				int index = dataArray[4].indexOf(",");
				String temp = dataArray[4].substring(index + 2);
				context.write(new TextIntPair(dataArray[0],0), new Text(temp));
			}
		}
	}
}

package task3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class JoinMapperTask3Job2 extends Mapper<Object, Text, Text, Text> {
	
	@Override
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {

		String[] temp = value.toString().split("\t");
		String place = temp[0].toLowerCase().replaceAll("[^A-Za-z0-9]", "");
		String[] tags = temp[1].toLowerCase().split("\\s+");
		for (String tag: tags) {
			if (tag.trim().length() > 0 && place.indexOf(tag) == -1) {
				context.write(new Text(temp[0]), new Text(tag));
			}
		}
	}

}

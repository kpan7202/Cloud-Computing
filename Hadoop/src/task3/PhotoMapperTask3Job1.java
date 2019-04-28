package task3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PhotoMapperTask3Job1 extends Mapper<Object, Text, TextIntPair, Text> {
	
	@Override
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {

		String[] dataArray = value.toString().split("\t");
		if (dataArray.length >= 5) {			
			String year = dataArray[3].substring(0, 4);
			/*String[] tags = dataArray[2].split("\\s+");
			for (String tag : tags) {
				if (!tag.trim().equals(year) &&  tag.trim().length() > 0) {
					context.write(new TextIntPair(dataArray[4],1), new Text(tag));
				}
			}*/
			String tags = (" " + dataArray[2] + " ").replaceAll(" " + year + " ", " ");
			context.write(new TextIntPair(dataArray[4],1), new Text(tags));
		}
		
	}

}

package task3;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducerTask3Job1 extends  Reducer<TextIntPair, Text, Text, Text> {

	@Override
	public void reduce(TextIntPair key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		//check if the key is coming from the place table
		Iterator<Text> valuesItr = values.iterator();
		if (key.getOrder().get() == 0){// the key is from the place table and filtered
			String placeName = valuesItr.next().toString();
			//String tags = "";
			StringBuilder tags = new StringBuilder();
			while (valuesItr.hasNext()){
				String tag = valuesItr.next().toString();
				/*if (placeName.toLowerCase().indexOf(tag.toLowerCase()) == -1) {
					tags += tag.toLowerCase() + " ";
				}*/
				/*String[] multiTags = valuesItr.next().toString().toLowerCase().split("\\s+");
				for (String tag : multiTags) {
					if (tag.trim().length() > 0 && placeName.toLowerCase().indexOf(tag) == -1) {
						tags += tag + " ";
					}
				}*/
				//tags += tag + " ";
				tags.append(tag + " ");
			}
			if (tags.length() > 0) {
				context.write(new Text(placeName), new Text(tags.toString()));
			}
		}
		
	}
	
}

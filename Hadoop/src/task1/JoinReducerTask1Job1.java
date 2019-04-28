package task1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducerTask1Job1 extends  Reducer<TextIntPair, Text, Text, Text> {

	@Override
	public void reduce(TextIntPair key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		//check if the key is coming from the place table
		Iterator<Text> valuesItr = values.iterator();
		if (key.getOrder().get() == 0){// the key is from the place table
			String placeName = valuesItr.next().toString();
			int ctr = 0;
			while (valuesItr.hasNext()){
				ctr += Integer.parseInt(valuesItr.next().toString());
			}
			if (ctr > 0) {
				context.write(new Text(placeName), new Text(String.valueOf(ctr)));
			}
		}
		
	}
}

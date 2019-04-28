package task1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducerTask1Job2 extends  Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		
			Iterator<Text> valuesItr = values.iterator();
			int ctr = 0;
			while (valuesItr.hasNext()){
				ctr += Integer.parseInt(valuesItr.next().toString());
			}
			context.write(key, new Text(String.valueOf(ctr)));
		
	}
}

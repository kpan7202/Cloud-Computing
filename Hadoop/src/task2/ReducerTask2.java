package task2;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerTask2 extends  Reducer<IntWritable, Text, Text, Text> {
	
	private TreeMap<Integer, String> map = new TreeMap<Integer, String>();
	private int maxSize = 50;

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {		
		Iterator<Text> valuesItr = values.iterator();
		while (valuesItr.hasNext()) {
			if (map.containsKey(key.get())) {
				map.put(key.get(), map.get(key.get()) + "\t" + valuesItr.next().toString());
			}
			else {
				map.put(key.get(), valuesItr.next().toString());
			}
			while (map.size() > maxSize) {				
				map.remove(map.firstKey());
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Integer[] arr = map.keySet().toArray(new Integer[map.size()]);
		int ctr = 0;
		for (int i = arr.length - 1; i >= 0 ; i--) {
			String val = map.get(arr[i]);
			String[] temp = val.split("\t");
			for (String locality : temp) {
				context.write(new Text(locality), new Text(String.valueOf(arr[i])));
				ctr++;
				if (ctr >= maxSize) {
					break;
				}
			}
			
			if (ctr >= maxSize) {
				break;
			}
		}
	}
}

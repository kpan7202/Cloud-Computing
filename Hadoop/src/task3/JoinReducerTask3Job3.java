package task3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.TreeMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducerTask3Job3 extends  Reducer<Text, Text, Text, Text> {
	private LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();

	@Override
	public void reduce(Text key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {

			Iterator<Text> valuesItr = values.iterator();
			map.put(key.toString(), map.get(key.toString()) + "\t" + valuesItr.next().toString());
	}
	
	@Override
	public void setup(Context context)	throws java.io.IOException, InterruptedException{
			
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (cacheFiles != null && cacheFiles.length > 0) {
			String line;
			String[] tokens;
			BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
			try {
				while ((line = reader.readLine()) != null) {
					tokens = line.split("\t");
					map.put(tokens[0], tokens[1]);
				}
			} 
			finally {
				reader.close();
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (String key : map.keySet()){
			context.write(new Text(key), new Text(map.get(key)));
		}
	}
	
}

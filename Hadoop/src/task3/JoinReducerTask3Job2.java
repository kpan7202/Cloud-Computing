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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducerTask3Job2 extends  Reducer<Text, Text, Text, Text> {
//	private LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
	private int maxTag = 10;

	@Override
	public void reduce(Text key, Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		
			Iterator<Text> valuesItr = values.iterator();
			HashMap<String, Integer> map2 = new HashMap<String, Integer>();
			while (valuesItr.hasNext()){
				String tag = valuesItr.next().toString();
				if (map2.containsKey(tag)) {
					map2.put(tag, map2.get(tag) + 1);
				}
				else {
					map2.put(tag, 1);
				}				
			}
			
			TreeMap<Integer, String> map3 = new TreeMap<Integer, String>();
			Iterator<String> iter = map2.keySet().iterator();
			while (iter.hasNext()) {
				String k = iter.next();
				Integer v = map2.get(k);
				if (map3.containsKey(v)) {
					map3.put(v, map3.get(v) + "\t" + k);
				}
				else {
					map3.put(v, k);
				}
				while (map3.size() > maxTag) {				
					map3.remove(map3.firstKey());
				}
			}
			
			Integer[] arr = map3.keySet().toArray(new Integer[map3.size()]);
			int ctr = 0;
			StringBuilder top10 = new StringBuilder();
			for (int i = arr.length - 1; i >= 0 ; i--) {
				String val = map3.get(arr[i]);
				String[] tags = val.split("\t");
				for (String tag : tags) {
					top10.append("(" + tag + ":" + arr[i] + ") ");
					ctr++;
					if (ctr >= maxTag) {
						break;
					}
				}
				
				if (ctr >= maxTag) {
					break;
				}
			}
			
//			map.put(key.toString(), map.get(key.toString()) + "\t" + top10);
			context.write(new Text(key), new Text(top10.toString()));
		
	}
	/*
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
	*/
}

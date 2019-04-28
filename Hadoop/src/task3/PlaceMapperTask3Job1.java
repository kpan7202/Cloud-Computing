package task3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class PlaceMapperTask3Job1 extends Mapper<Object, Text, TextIntPair, Text> {
	private ArrayList<String> list = new ArrayList<String>();
	
	@Override
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {

		String[] dataArray = value.toString().split("\t");
		if (dataArray.length >= 6) {
			if (dataArray[5].equals("7")) {
				if (list.indexOf(dataArray[4]) >= 0) {
					context.write(new TextIntPair(dataArray[0],0), new Text(dataArray[4]));
				}
			}
			else if (dataArray[5].equals("22")) {
				int index = dataArray[4].indexOf(",");
				String temp = dataArray[4].substring(index + 2);
				if (list.indexOf(temp) >= 0) {
					context.write(new TextIntPair(dataArray[0],0), new Text(temp));
				}
			}
		}
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
					list.add(tokens[0]);
				}
			} 
			finally {
				reader.close();
			}
		}
	}

}

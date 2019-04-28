package task3;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class JoinPartitionerTask3Job1 extends Partitioner<TextIntPair,Text> {

	@Override
	public int getPartition(TextIntPair key, Text value, int numPartition) {
		// TODO Auto-generated method stub
		return (key.getKey().hashCode() & Integer.MAX_VALUE) % numPartition;
	}
		
}

package task1;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class JoinPartitionerTask1Job2 extends Partitioner<Text,Text> {

	@Override
	public int getPartition(Text key, Text value, int numPartition) {
		String str = key.toString().substring(0, 1);	
		if (str.equalsIgnoreCase("A") || str.equalsIgnoreCase("B") || str.equalsIgnoreCase("C") ||
				str.equalsIgnoreCase("D") || str.equalsIgnoreCase("E")) {
			return 0;
		}
		else if (str.equalsIgnoreCase("F") || str.equalsIgnoreCase("G") || str.equalsIgnoreCase("H") ||
				str.equalsIgnoreCase("I") || str.equalsIgnoreCase("J")) {
			return 1;
		}
		else if (str.equalsIgnoreCase("K") || str.equalsIgnoreCase("L") || str.equalsIgnoreCase("M") ||
				str.equalsIgnoreCase("N") || str.equalsIgnoreCase("O")) {
			return 2;
		}
		else if (str.equalsIgnoreCase("P") || str.equalsIgnoreCase("Q") || str.equalsIgnoreCase("R") ||
				str.equalsIgnoreCase("S") || str.equalsIgnoreCase("T")) {
			return 3;
		}
		else {
			return 4;
		}
	}
		
}

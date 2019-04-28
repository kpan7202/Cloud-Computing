package task1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinGroupComparatorTask1Job1 extends WritableComparator {

	protected JoinGroupComparatorTask1Job1() {
		super(TextIntPair.class,true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TextIntPair tip1 = (TextIntPair) w1;
		TextIntPair tip2 = (TextIntPair) w2;
		return tip1.getKey().compareTo(tip2.getKey());
	}
	
}

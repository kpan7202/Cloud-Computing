package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class JoinDriverTask1 {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: JoinDriverTask1 <photoin> <placein> <out> <num of reducers>");
			System.exit(2);
		}
		Job job1 = new Job(conf, "join driver task 1 job 1");
		job1.setNumReduceTasks(Integer.parseInt(otherArgs[3])); 
		job1.setJarByClass(JoinDriverTask1.class);
		
		Path photoTable = new Path(otherArgs[0]);
		Path placeTable = new Path(otherArgs[1]);		
		Path tmpJob1 = new Path("tmpJob1");
		Path finalOutput = new Path(otherArgs[2]);
		
		MultipleInputs.addInputPath(job1, placeTable, TextInputFormat.class,PlaceMapperTask1Job1.class);
		MultipleInputs.addInputPath(job1, photoTable, TextInputFormat.class,PhotoMapperTask1Job1.class);		
		FileOutputFormat.setOutputPath(job1, tmpJob1);
		job1.setMapOutputKeyClass(TextIntPair.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setGroupingComparatorClass(JoinGroupComparatorTask1Job1.class);
		job1.setPartitionerClass(JoinPartitionerTask1Job1.class);
		job1.setReducerClass(JoinReducerTask1Job1.class);
		
		if (job1.waitForCompletion(true)) {
			Job job2 = new Job(conf, "join driver task 1 job 2");
			job2.setNumReduceTasks(5); 
			job2.setJarByClass(JoinDriverTask1.class);
			job2.setMapperClass(JoinMapperTask1Job2.class);
		    job2.setReducerClass(JoinReducerTask1Job2.class);
		    job2.setPartitionerClass(JoinPartitionerTask1Job2.class);
		    job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
		    TextInputFormat.addInputPath(job2, tmpJob1);
		    FileOutputFormat.setOutputPath(job2, finalOutput);
		   
		    if (job2.waitForCompletion(true)) {
		    	FileSystem.get(conf).delete(tmpJob1, true);
		    }
		      
		}
	}
}

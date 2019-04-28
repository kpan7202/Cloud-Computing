package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class JoinDriverTask3 {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 5) {
			System.err.println("Usage: JoinDriverTask3 <photoin> <placein> <task2out> <out> <num of reducers>");
			System.exit(2);
		}
		
		Path photoTable = new Path(otherArgs[0]);
		Path placeTable = new Path(otherArgs[1]);	
		Path tmpJob2 = new Path("tmpJob2");
		Path tmpJob3 = new Path("tmpJob3");
		Path task2out = new Path(otherArgs[2]);
		Path finalOutput = new Path(otherArgs[3]);
		
		Job job1 = new Job(conf, "join driver task 3 job 1");
		DistributedCache.addCacheFile(task2out.toUri(),job1.getConfiguration());
		job1.setNumReduceTasks(Integer.parseInt(otherArgs[4]));
		job1.setJarByClass(JoinDriverTask3.class);
		
		MultipleInputs.addInputPath(job1, placeTable, TextInputFormat.class,PlaceMapperTask3Job1.class);
		MultipleInputs.addInputPath(job1, photoTable, TextInputFormat.class,PhotoMapperTask3Job1.class);		
		FileOutputFormat.setOutputPath(job1, tmpJob2);
		job1.setMapOutputKeyClass(TextIntPair.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setGroupingComparatorClass(JoinGroupComparatorTask3Job1.class);
		job1.setPartitionerClass(JoinPartitionerTask3Job1.class);
		job1.setReducerClass(JoinReducerTask3Job1.class);
		
		if (job1.waitForCompletion(true)) {
			Job job2 = new Job(conf, "join driver task 3 job 2");
//			DistributedCache.addCacheFile(task2out.toUri(),job2.getConfiguration());
			job2.setNumReduceTasks(Integer.parseInt(otherArgs[4])); 
			job2.setJarByClass(JoinDriverTask3.class);
			job2.setMapperClass(JoinMapperTask3Job2.class);
		    job2.setReducerClass(JoinReducerTask3Job2.class);
		    job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
		    TextInputFormat.addInputPath(job2, tmpJob2);
		    FileOutputFormat.setOutputPath(job2, tmpJob3);
		   
		    if (job2.waitForCompletion(true)) {
		    	Job job3 = new Job(conf, "join driver task 3 job 3");
				DistributedCache.addCacheFile(task2out.toUri(),job3.getConfiguration());
				job3.setNumReduceTasks(1); 
				job3.setJarByClass(JoinDriverTask3.class);
				job3.setMapperClass(JoinMapperTask3Job3.class);
				job3.setReducerClass(JoinReducerTask3Job3.class);
				job3.setMapOutputKeyClass(Text.class);
				job3.setMapOutputValueClass(Text.class);
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);
			    TextInputFormat.addInputPath(job3, tmpJob3);
			    FileOutputFormat.setOutputPath(job3, finalOutput);
			    
			    if (job3.waitForCompletion(true)) {
			    	FileSystem.get(conf).delete(tmpJob2, true);
			    	FileSystem.get(conf).delete(tmpJob3, true);
			    }
		    }
		}

	}
}

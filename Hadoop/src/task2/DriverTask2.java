package task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class DriverTask2 {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: DriverTask2 <in1> <out>");
			System.exit(2);
		}
		Path input = new Path(otherArgs[0]);		
		Path output = new Path(otherArgs[1]);
		
		Job job = new Job(conf, "driver task 2");
		job.setNumReduceTasks(1); 
		job.setJarByClass(DriverTask2.class);
		TextInputFormat.addInputPath(job, input);
		TextOutputFormat.setOutputPath(job, output);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MapperTask2.class);
		job.setCombinerClass(CombinerTask2.class);
		job.setReducerClass(ReducerTask2.class);		
		job.waitForCompletion(true);

	}
}

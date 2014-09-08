
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CounterCount {
	static enum UpdateCount{ //define counters
		CNT
	}
	static long c = 0;

	public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
		NullWritable out = NullWritable.get();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, out);
		}
	}


public static class Reduce extends	Reducer<Text, NullWritable, NullWritable, NullWritable> {
	NullWritable out = NullWritable.get();
	public void reduce(Text key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		context.getCounter(UpdateCount.CNT).increment(1); //Increment counters
		context.write(out, out);
		
	}
}

public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	if (fs.exists(new Path(args[1]))) {
		fs.delete(new Path(args[1]), true);
	}

	Job job = new Job(conf, "wordcount");
	job.setJarByClass(CounterCount.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(NullWritable.class);
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(NullWritable.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);

	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	boolean success = job.waitForCompletion(true);
	c = job.getCounters().findCounter(UpdateCount.CNT).getValue(); // Get counet values
	System.out.println("Total keys or lines processed: "+c);
	System.exit(success ? 0 : 1);
}

}

package space.yixian.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ItemBasedCFDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		
		Configuration configuration  = new Configuration();
		configuration.set("mapreduce.job.jar","CF.jar");
		
		Job job1 = Job.getInstance(configuration);
		
		job1.setJobName("CombineRates");
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		
		FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:8020/u.data")); //your path
   		FileOutputFormat.setOutputPath(job1, new Path("hdfs://localhost:8020/CF/output")); //your path
		
   		job1.waitForCompletion(true);
   		
   		
   		Job job2 = Job.getInstance(configuration);
		
		job2.setJobName("CalculateSimilarity");
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		
		FileInputFormat.addInputPath(job2, new Path("hdfs://localhost:8020/CF/output/part*")); //the output path of job1
   		FileOutputFormat.setOutputPath(job2, new Path("hdfs://localhost:8020/CF/output/job2")); //your path
		
   		
		return job2.waitForCompletion(true)?0:1;
   		//return 0;
	}

	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ItemBasedCFDriver(), args);		
		System.exit(res);
	}
}



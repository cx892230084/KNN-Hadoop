package space.yixian.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Scanner;

import org.apache.commons.math3.optimization.linear.AbstractLinearOptimizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


public class SpecifiedUserRecommendationDriver extends Configured implements Tool{


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
//			int res = ToolRunner.run(new Configuration(), new CalSimilarityMatrixDriver(), args);		
//			System.exit(res);
			
			Integer userId = 0, k = 10;	
			Scanner scanner = new Scanner(System.in); // input the userID(from 1 to 943)

			while(userId < 1 || userId > 943){
				
				if(!scanner.hasNextInt()){
					System.out.println("Please input a number");
					scanner.nextLine();
				}else{
					userId = scanner.nextInt();
					if(userId < 1 || userId > 943){
						System.out.println("Please input a number(0-943)");
					}
				}
			}
			

			Configuration configuration = new Configuration();
			configuration.set("fs.defalutFS","hdfs://localhost:8088");
			FileSystem fs = FileSystem.get(configuration);
			
			
			FSDataInputStream inputStream = fs.open(new Path("hdfs://localhost:8020/u.data"));
			
			String newFileAdd = "hdfs://localhost:8020/SelectedUser";
			FSDataOutputStream outputStream = fs.create(new Path(newFileAdd));
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
			
			int line = 0;
			String aLine = null;
			//开数组
			while (( aLine = reader.readLine() ) !=null) {
				//System.out.println(aLine);
				Integer user = Integer.valueOf(aLine.split(" |\t")[0]);
				if(user == userId){
					line++;
					//writer.write(line + "\t" + "1" + aLine.split("\t")[2]);
					
					System.out.println(line + "\t" + "1" + "\t"+aLine.split("\t")[2] +"--"+ aLine);
				}				
			}
		}
	}




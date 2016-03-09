package space.yixian.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.commons.math3.optimization.linear.AbstractLinearOptimizer;
import org.apache.hadoop.RandomTextWriterJob.RandomTextMapper;
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
import org.apache.hadoop.util.ToolRunner;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;


public class SpecifiedUserRecommendation{
		

	private final static Integer MOVIE_SUM = 1682;
	private final static Integer USER_SUM = 943;
	private static Scanner scanner;
		
	
		
	public static void calUserMatrix(){
		Integer userId = 0;	
		scanner = new Scanner(System.in);

		//input the user that you want to predict
		while(userId < 1 || userId > USER_SUM){
			
			if(!scanner.hasNextInt()){
				System.out.println("Please input a number");
				scanner.nextLine();
			}else{
				userId = scanner.nextInt();
				if(userId < 1 || userId > USER_SUM){
					System.out.println("Please input a number(0-943)");
				}
			}
		}
		

		Configuration configuration = new Configuration();
		configuration.set("fs.defalutFS","hdfs://localhost:8088");
		FileSystem fs;
		BufferedReader reader = null;
		BufferedWriter writer = null;
		
		try {
			fs = FileSystem.get(configuration);
		
			FSDataInputStream inputStream = fs.open(new Path("hdfs://localhost:8020/u.data"));
			
			String newFileAdd = "hdfs://localhost:8020/CF/M2";
			FSDataOutputStream outputStream = fs.create(new Path(newFileAdd));
			
			reader = new BufferedReader(new InputStreamReader(inputStream));
			writer = new BufferedWriter(new OutputStreamWriter(outputStream));
		
			
				
			 
			/**
			 * 生成指定用户的评分矩阵 	form the specified user's rated matrix 
			 * 矩阵存储格式	Matrix format: movieID \t columnNumber \t rate
			 * MovieID is the row number.
			 * If you want to calculate N users simultaneously, 
			 * you can change the code to read N userIds and use N columns in the matrix.
			 */

			String aLine = null;
			while ((aLine = reader.readLine() ) !=null) {
				String[] split = aLine.split(" |\t");
				Integer user = Integer.valueOf(split[0]);
				
				if(user == userId){
					String rate = split[2]; 
					String movieId = split[1];
			
					//movieID  columnNumber(ONE user)  rate
					writer.write(movieId + "\t" + "1" + "\t" + rate + "\n" ); 
				}				
			}

			
			
		} catch (IOException e) {
			System.out.println("cannot open the file");
			e.printStackTrace();
		}finally{
			try {
				writer.close();
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("cannot close the file");
			}
		}
		
	}

		
		
		
		
		
	public static void main(String[] args) throws Exception {		
		
		//calculate the similarity matrix M1
//		int res = ToolRunner.run(new Configuration(), new SimilarityMatrixDriver(), args);		
//		if(res == 1) System.exit(1);
		
		//calculate the user matrix M2
//		System.out.println("Please input a userID(0-943) who you want to predict:");
//		calUserMatrix();
		
		//calculate M1*M2
		int res1 = ToolRunner.run(new Configuration(), new MulMatrixDriver(), args);		
		if(res1 == 1) System.exit(1);
		
	}
			
}



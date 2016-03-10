package space.yixian.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.commons.net.ftp.parser.MacOsPeterFTPEntryParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.I;



public class SpecifiedUserRecommendation{
		

	private final static Integer MOVIE_SUM = 1682;
	private final static Integer USER_SUM = 943;
	private static Scanner scanner;
		
	
		
	public static void calUserMatrix(String inputAdd, String outAdd){
		System.out.println("Please input a userID(0-943) who you want to predict:");
		
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
		
			FSDataInputStream inputStream = fs.open(new Path(inputAdd));
			
			FSDataOutputStream outputStream = fs.create(new Path(outAdd));
			
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

		
	public static void calTopK(String address){
		System.out.println("Please input the top k(1-100) in result:");
		
		Integer k = 0;
		int max = 1000;
		scanner = new Scanner(System.in);

		while(k < 1 || k > max){
			
			if(!scanner.hasNextInt()){
				System.out.println("Please input a number");
				scanner.nextLine();
			}else{
				k = scanner.nextInt();
				if(k < 1 || k > max){
					System.out.println("Please input a number(0-100)");
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
		
			FSDataInputStream inputStream = fs.open(new Path(address));	
			reader = new BufferedReader(new InputStreamReader(inputStream));
						
			
			HashMap<Integer, Double> map = new HashMap<>();
			ArrayList<Map.Entry<Integer, Double>> arrayList;
			
			String aLine = null;
			while ((aLine = reader.readLine() ) !=null) {
				String[] split = aLine.split("\t");
				int movieID = Integer.valueOf(split[0]);
				Double prediction = Double.valueOf(split[1]);	
				
				if(!prediction.equals(0)){ // not yet rated 
					map.put(movieID, prediction);}			
			}
			
			arrayList = new ArrayList<Map.Entry<Integer,Double>>(map.entrySet());
			
			
			Collections.sort(arrayList, new Comparator<Map.Entry<Integer, Double>>() {
				
				@Override
				public int compare(Entry<Integer, Double> o1, Entry<Integer, Double> o2) {
					return o1.getValue().doubleValue() == o2.getValue().doubleValue() ? 0
							:  (o1.getValue().doubleValue() < o2.getValue().doubleValue() ? 1 : -1);
				};
				
			});
			
			for(int i = 0 ; i < k ; i++){
				System.out.println("Top"+ (i+1) +"  Movie id:"+arrayList.get(i).getKey()+"  Similarity:"+arrayList.get(i).getValue());
			}
					
			
			
			
		} catch (IOException e) {
			System.out.println("cannot open the file");
			e.printStackTrace();
		}finally{
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("cannot close the file");
			}
		}
		
		
		
		
	}
		
		
		
	public static void main(String[] args) throws Exception {		
		
		
		String ipAddress = "hdfs://localhost:8020/";
		
		String[] address = {ipAddress+"u.data", ipAddress+"CF/job1out", ipAddress+"CF/M1"}; 
		//1.calculate the similarity matrix M1 
		int res = ToolRunner.run(new Configuration(), new SimilarityMatrixDriver(), address);		
		if(res == 1) System.exit(1);
		 
		
		//2.calculate the user matrix M2	
		calUserMatrix(ipAddress+"u.data",ipAddress+"CF/M2");
		
		
		//3.calculate M1*M2
		String[] address1 = {ipAddress+"CF/M*",ipAddress+"CF/Result"}; 
		int res1 = ToolRunner.run(new Configuration(), new MulMatrixDriver(), address1);		
		if(res1 == 1) System.exit(1);
		
		
		//4.sort the result and get k neighbor   
		String resultAddress = ipAddress+ "CF/Result" + "/part-r-00000";
		calTopK(resultAddress);
	
		
	}
			
}



package space.yixian.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochRequestProto;

public class Recommendation {
	
	private FileSystem fs;
	
	/**
	 * get HDFS for downloading the output file
	 * @throws IOException
	 */
	void initFS() throws IOException{
		Configuration configuration = new Configuration();
		configuration.set("fs.defalutFS","hdfs://localhost:8088");
		fs = FileSystem.get(configuration);
	}
	
	
	/**
	 * down the output files(part-r-00000,part-r-00001...) from HDFS to local
	 * @throws IOException
	 */
	void downOutputFiles() throws IOException{
		
		if(fs != null){
		//get all the output files begin with "part-r-"
			RemoteIterator<LocatedFileStatus> filelist = fs.listFiles(new Path("hdfs://localhost:8020/CF/output/job2/"), true);
		
			while (filelist.hasNext()){	
		
				LocatedFileStatus file = filelist.next();
				
				if(file.getPath().getName().startsWith("part-r-")){ 	
					fs.copyToLocalFile(new Path("hdfs://localhost:8020/CF/output/job2/"+file.getPath().getName()), new Path("/home/may/data/CF"));	
					
				}
			}
		}
	}
		
	
	
	
	
	
	public static void main(String[] args)  {
		
		//Recommandation r = new Recommandation();
		//r.initFS();
		//r.downOutputFiles();   
		
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
		
		File similarityFile = new File("/home/may/data/CF/part-r-00000");
		File userFile = new File("/home/may/data/CF/u.data");
		String aLine;
		double[][] similarityMatrix = new double[1682][1682];
		
		BufferedReader bufferedReader = null;
		BufferedReader bufferedReader2 = null;
		try {
			bufferedReader = new BufferedReader(new FileReader(similarityFile));
			
			while((aLine = bufferedReader.readLine()) != null){
				
				// a line: moiveA-movieB \t similarity 
				String[] movAndRate = aLine.split("\t"); 
				String[] movPair = movAndRate[0].split("-"); // moiveA movieB
				
				similarityMatrix[Integer.valueOf(movPair[0])-1][Integer.valueOf(movPair[1])-1] = Double.valueOf(movAndRate[1]);					
			}
			
			//find which movies rated by userid
			bufferedReader2 = new BufferedReader(new FileReader(userFile));
			HashMap<String, String> userMovieMap = new HashMap<>();
			
			//a line: userId movieId rate timestamp 
			while((aLine = bufferedReader2.readLine()) != null){
				userMovieMap.put(aLine.split("\t")[0], aLine.split("\t")[1]);
			}
			
			
			//...
			
		} catch (FileNotFoundException e) {
			System.out.println("file no found");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("reading err");
			e.printStackTrace();
		}finally {
			try {
				bufferedReader.close();
				bufferedReader2.close();
			} catch (IOException e) {
				System.out.println("close err");
				e.printStackTrace();
			}
		}
		
	
		   
		  
		
		
	}
	
}

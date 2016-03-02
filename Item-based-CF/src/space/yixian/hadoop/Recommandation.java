package space.yixian.hadoop;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;


public class Recommandation {
	
	/**
	 * down the output file from HDFS to local
	 * @throws IOException
	 */
	void downloadFile() throws IOException{
		
		Configuration configuration = new Configuration();
		configuration.set("fs.defalutFS","hdfs://localhost:8088");
		FileSystem fs = FileSystem.get(configuration);
		
		//get all the output files begin with "part-r-"
		RemoteIterator<LocatedFileStatus> filelist = fs.listFiles(new Path("hdfs://localhost:8020/CF/output/job2/"), true);
		
		while (filelist.hasNext()){	
			LocatedFileStatus file = filelist.next();
			if(file.getPath().getName().startsWith("part-r-")){ 
				fs.copyToLocalFile(new Path("hdfs://localhost:8020/CF/output/job2/"+file.getPath().getName()), new Path("/home/may/data/CF"));	
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		Recommandation r = new Recommandation();
		r.downloadFile();
				
	}
	
}

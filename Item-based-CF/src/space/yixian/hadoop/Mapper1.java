package space.yixian.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<Object, Text, IntWritable, Text>{
		
	/**
	 * 原数据格式 original data format:  < userId,movieId,rate,timestamp >
	 * 输出 output: < userId，movieId-rate > 
	 */
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		String[] spilt = value.toString().split("\t| ");
		String userid = spilt[0];
		String movieid = spilt[1];
		String rate = spilt[2];
		
		//得到一行的内容，进行切分，输出为<用户id，电影id-对应评分>
		//get a line of the data set, and split it into <userId，movieId-rate>
		context.write(new IntWritable(Integer.valueOf(userid)), new Text(movieid+"-"+rate));	
		
	}
	
	
}

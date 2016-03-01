package space.yixian.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<Object, Text, IntWritable, Text>{
	private IntWritable userId = new IntWritable();
	private Text MovAndRate = new Text();
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		
		//得到一行的内容，进行切分，输出为<用户id，电影id-对应评分>
		//get a line of the data set, and split it into <userId，movieId-rate>
		while( tokenizer.hasMoreTokens() ){
			
			userId.set(Integer.valueOf(tokenizer.nextToken()));
			MovAndRate.set(tokenizer.nextToken() + "-" + tokenizer.nextToken());  //<userId，movieId-rate>
			tokenizer.nextToken();//忽略评论时间 ignore the timestamp
			
			context.write(userId, MovAndRate);
			
		}
	}
	
	
}

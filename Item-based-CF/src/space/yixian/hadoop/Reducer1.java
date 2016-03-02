package space.yixian.hadoop;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<IntWritable, Text, IntWritable, Text>{

	/**
	 * 输入 input: < userId，movieId-rate-list >
	 * 输出 output: < userId, moiveId1-rate1 moiveId2-rate2 ... moiveIdN-rateN >
	 */
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		StringBuilder allRates = new StringBuilder();
		
		for(Text val : values){
			//一个用户的所有评分，用“ ”分割 	 using " " to split all the rates of one user
			allRates.append( val + " " );
		}
		
		//输出 output: <userid,moiveId1-rate1 moiveId2-rate2 ... moiveIdN-rateN>
		context.write(key, new Text(allRates.toString().trim()));
	}
	
}

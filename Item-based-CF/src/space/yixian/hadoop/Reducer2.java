package space.yixian.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.inject.Key;

public class Reducer2 extends Reducer<Text, Text, Text, Text> {
	
	/**
	 * 输入 input :< moiveA movieB, rateA rateB list>
	 * 输出 output:< M1	moiveA	movieB, similarity >
	 * 采用Cosine相似度
	 * Using Cosine Similarity 
	 */
	@Override
	protected void reduce(Text movies, Iterable<Text> rates, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
			
		Double rateMul = new Double(0);
		Double sqrtSum1 = new Double(0);
		Double sqrtSum2 = new Double(0);
		Double similarity = new Double(0);
		
		for(Text rate : rates){
			
			String[] ratePair = rate.toString().split("-");
			Double rateA = Double.valueOf(ratePair[0]);
			Double rateB = Double.valueOf(ratePair[1]);
			
			rateMul += rateA * rateB;
			sqrtSum1 += rateA * rateA;
			sqrtSum2 += rateB * rateB;
			
		}
		
		if(sqrtSum1 == 0 || sqrtSum2 == 0){
			context.write(movies, new Text("0")); 
			
		}else{
			similarity = rateMul / (Math.sqrt(sqrtSum1) * Math.sqrt(sqrtSum2));//Cosine Similarity 
			
			//M1 is the flag of the first matrix in matrix multiplication of MulMatrixMapper3 
			//M1 作为MulMatrixMapper3在做矩阵乘法计算时，对第一个矩阵的标识（M1*M2=M3）
			context.write(new Text("M1\t"+movies), new Text(similarity.toString())); 
			
		}
		
	}
}

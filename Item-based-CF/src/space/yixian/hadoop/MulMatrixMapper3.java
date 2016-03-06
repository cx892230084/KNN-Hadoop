package space.yixian.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * input :
 * M1 context.write(new Text("M1\t"+movies), new Text(similarity.toString())); 
 * 		M1	mA(row)-mb(column)	double
 * M2 writer.write( "M2\t"+ (i+1) + "\t" + "1" + "\t" + userMatrix[i]); 
 * 		M2	row	 column(1:because we just calculate one user's prediction)	 rate
 * @author may
 *
 */
public class MulMatrixMapper3 extends Mapper<Text, Text, Text, Text> {
	
}

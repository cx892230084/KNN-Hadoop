package space.yixian.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



/**
 * input file:
 * M1:   moiveA(row)		 movieB(column)		similarity
 * M2:	movieId(row)	 userNum(1:because now just calculate ONE user's prediction)		 rate
 * 
 * output:
 * KEY: <i,j> of M3 
 * 		M1*M2=M3,  and M3<i,j> is one element of matrix of the product. 
 * 		key是乘积得到的C矩阵的一个元素。
 * 
 * VALUE: <"M1",column,similarity> from row i of M1  ||   <"M2",movieId,rate> from column j of M2
 *      M1 or M2 use to sign which matrix(1 or 2) the element come form
 *      calculating the element M3<i,j> requires the every element in row i of M1 and column j of M2
 *      M1或者M2用来标识值来自哪个矩阵，矩阵1还是矩阵2
 *      计算key的<M3i,M3j>的结果，需要M1第i行和M2第j行的所有元素，所以此处value给出所有计算时所需元素
 *      
 * e.g.:
 *  	M1(2*4)  *  M2(4*3) =  M3(2*3) 
 *  
 * 		Element M1<2,1> is required when calculate THREE elements: M3<2,1> , M3<2,2> and M3<2,3>. 
 * 		(The number THREE equals to M2 column number)	
 *      So after reading a line of M1 and get the M1<rowNum,columnNum,value> : [2,1, value of M1<2,1>]
 *      we can set the KEY in Mapper as <rowNum, from 1 ro M2 column number> : <2,1> , <2,2> and <2,THREE>,
 *      and set the VALUE in Mapper as <"M1",columnNum,value> :  ["M1",1, value of M1<2,1>]
 *      
 * @author may
 *
 */


public class MulMatrixMapper3 extends Mapper<Text, Text, Text, Text> {
	
	private int M1RowNum = 1682; // Similarity Matrix's row number 
	private int M2ColNum = 1;  // User Matrix's column number
		
	
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		FileSplit inputSplit =  (FileSplit) context.getInputSplit();
		String path = inputSplit.getPath().toString();
		String[] tokenize = value.toString().split("\t");
		String rowNum, columnNum, mValue;
		
		if(path.contains("M1")){ // moiveA(row)		 movieB(column)		similarity
			rowNum = tokenize[0];
			columnNum = tokenize[1];
			mValue = tokenize[2];
			
			for(int colIDX = 1 ; colIDX <= M2ColNum; colIDX++ ){
				//key<rowNum, from 1 ro M2 column number> in M3
				//value<"M1",columnNum,similarity> 				
				context.write(new Text(rowNum +","+ colIDX), new Text("M1,"+columnNum+","+mValue)); 	
			}
			
			
		}else if(path.contains("M2")){ //movieId(row)  userNum(==1)	 rate
			rowNum = tokenize[0];
			columnNum = tokenize[1];
			mValue = tokenize[2];
			
			for(int rowIDX = 1; rowIDX <= M1RowNum; rowIDX++)
			context.write(new Text(rowNum+","+columnNum), new Text("M2,"+rowNum+","+mValue));
		}
		
	}
	
}

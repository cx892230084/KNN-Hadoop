	package space.yixian.hadoop;
	
	import java.io.IOException;
	import java.util.HashMap;
	import java.util.Iterator;

	import org.apache.hadoop.io.DoubleWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Reducer;
	
	/**
	 * input key: M3<i,j> 
	 * input value: all the required elements in M1,M2 to calculate the M3<i,j>
	 * 
	 * output key: M3<i,j>
	 * output value: value of M3<i,j>
	 * 
	 * calculate process & e.g.:
	 * 		key : M3<1,1> 
	 * 		value : <M1,1,10> <M2,1,5> // if the second number of them is the same, multiply the value'': 10*5
	 *  			<M1,2,20> 		   //if no matched number in the other matrix, ignore this line 
	 *   			<M1,3,30> <M2,3,10> //30*10
	 *   	
	 *   	add the products together to get the value of M3<1,1> = 10*5 + 30*10 = 350
	 *   	
	 *   (no  <M2,2,value>, because assume that the value equal to 0, 
	 *   	it would not store in the files and not be wrote by program )	
	 * 
	 * 
	 * @author may
	 *
	 */
	public class MulMatrixReducer3 extends Reducer<Text, Text, Text, DoubleWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
				
			HashMap<String, String> map1 = new HashMap<String,String>();
			HashMap<String, String> map2 = new HashMap<String,String>();
			
			
			for(Text value : values){
				String[] split = value.toString().split(",");
				String flag = split[0];
				String idx = split[1];
				String mValue = split[2];
				
				if(flag.equals("M1")){
					map1.put(idx, mValue);
				}else if(flag.equals("M2")){
					map2.put(idx,mValue);
				}
			}
			
			
			Double sum = new Double(0);
			Iterator<String> iterator = map1.keySet().iterator();
			
			while(iterator.hasNext()){
				String cur = iterator.next();
				String valueA = map1.get(cur);
				String valueB = map2.get(cur);
				if(valueA != null && valueB != null)
					sum += Double.valueOf(valueA) + Double.valueOf(valueB);
			}
			
			context.write(key, new DoubleWritable(sum));		
		}
		
	}

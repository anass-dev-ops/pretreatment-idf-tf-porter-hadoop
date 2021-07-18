package pretraitement_v8;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<Object, Text, Text, Text>{
	
	
	private Text value2 = new Text();
    private Text key2 = new Text();
	
	 @Override
	    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
	            throws IOException, InterruptedException {
		 
		 //System.out.println("KEY = "+key.toString()+"  ;;; Value = "+value.toString());
		 
		 String [] str = value.toString().split("\t");
		 String [] str2 = str[0].split(":");

		 
		 //System.out.println("Test key  = "+ str[0]+"Test Value = "+str[1]);
		 
		 
//		 key2.set(str[0]);
//		 value2.set(str[1]);
		 
		 
		 key2.set(str2[0]);
		 value2.set(str2[1]+":"+str[1]);
		 
	     context.write(key2, value2);

	 }

}

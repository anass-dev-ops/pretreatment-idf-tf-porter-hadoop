package pretraitement_v8;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PREPMapper2 extends Mapper<Object, Text, Text, Text> {
	
	
	private Text value2 = new Text();
    private Text key2 = new Text();
    
    String nbrMots;
	
	 @Override
	    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
	            throws IOException, InterruptedException {
		 
	        if (value.toString().startsWith("!")) 
	        {
	        	String [] nbrM = value.toString().split("\t");
	        	nbrMots = nbrM[1];
	        	
	            return;

	        }

		 
		 String [] str = value.toString().split("\t");
		 String [] str2 = str[1].split(":");
		 
		 
		 key2.set(str[0]);
		 value2.set(str2[0]+":"+str2[1]);
		 
	     context.write(key2, value2);
		 
	 }

	 @Override
	    public void cleanup(Context context) throws IOException,InterruptedException 
	    {
	    	//System.out.println("\n\n\n\t N = "+idword+"\n\n\n");
	    	context.write(new Text("!nbrMots"), new Text(nbrMots));
	    }
}

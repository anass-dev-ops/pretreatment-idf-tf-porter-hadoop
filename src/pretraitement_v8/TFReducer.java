package pretraitement_v8;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TFReducer extends Reducer<Text, Text, Text, Text>{
	
	

	
	
	@Override
    public void setup(Context context)
    {

    }

	 @Override
	    protected void reduce(Text key, Iterable<Text> values,
	            Reducer<Text, Text, Text, Text>.Context context)
	                    throws IOException, InterruptedException {
	        if (values == null) {
	            return;
	        }
	        
	        for (Text value : values) {
	            context.write(key, value);
	        }
	 }
    
    @Override
    public void cleanup(Context context) throws IOException,InterruptedException 
    {
    	
		
    }

}

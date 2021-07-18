package pretraitement_v8;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;





public class TFCombiner extends Reducer<Text, Text, Text, Text> {


	
private int allWordCount = 0; 
private Text label = new Text();


    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
         
    	//System.out.println("\nKEY : "+key.toString());
        if (values == null) {
            return;
        }
        
    	//System.out.println("\n TESTSTSTSTSTSTSTSTS  "+values.iterator().next().toString());
        
        if(key.toString().endsWith("!")) {
            allWordCount = Integer.parseInt(values.iterator().next().toString());
            return;
        }
        
        
        int sumCount = 0;
        for (Text value : values) {
            sumCount += Integer.parseInt(value.toString());
        }
        
        //double tf = Math.log10(1.0 * sumCount / allWordCount)+1;
        double tf = 1.0 * sumCount / allWordCount;
        //System.out.print("sumCount = "+sumCount+" , allWordCount = "+allWordCount);
        
    	//System.out.println("\n Key =  "+key+"   Value = "+String.valueOf(tf));
        String[] str = key.toString().split(":");
        label.set(String.join(":",  str[1], str[0]));

        context.write(label, new Text(String.valueOf(tf)));
    }

	
	
	@Override
    public void cleanup(Context context) throws IOException,InterruptedException 
    {	
		
    }
}

package pretraitement_v8;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IDFReducer extends Reducer<Text, Text, Text, Text> {
    
    private Text label = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
    	
    	
    	
        
        if (values == null) {
            return;
        }
        
        int fileCount = 0;
        for (Text value : values) {
            fileCount += Integer.parseInt(value.toString());
        	//System.out.println("=====>  Key = "+ key.toString() + "   Value =  "+value.toString());

        }
        
    	//System.out.println("\t\n"+key + "     "+ fileCount);

        
        label.set(String.join(":", key.toString(), "!"));
        
        int totalFileCount = Integer.parseInt(context.getProfileParams()) - 1;
        double idfValue = Math.log10(1.0 * 3000 / (fileCount));//+ 1));
        
    	//System.out.println("\t\n"+ "totalFileCount =  "+totalFileCount+"idf  = "+idfValue);

        context.write(label, new Text(String.valueOf(idfValue)));
    }
}


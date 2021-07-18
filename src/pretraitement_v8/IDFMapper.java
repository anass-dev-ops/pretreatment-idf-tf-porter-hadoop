package pretraitement_v8;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class IDFMapper extends Mapper<Object, Text, Text, Text> {
    
    private final Text one = new Text("1");
    private Text label = new Text();
    
    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
    	
    	
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        label.set(tokenizer.nextToken().split(":")[0]);
        
        //System.out.println(label +"     hhhhhh   "+one);
        context.write(label, one);
    }
}

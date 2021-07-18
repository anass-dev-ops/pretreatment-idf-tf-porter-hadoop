package pretraitement_v8;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IntegrateMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
    	    	
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        //System.out.println("======================> valueee  "+value.toString());
    	//System.out.println("\t\n\n"+new Text(tokenizer.nextToken()) +"  ggggg   "+new Text(tokenizer.nextToken()));

        context.write(new Text(tokenizer.nextToken()), new Text(tokenizer.nextToken()));
    }
}

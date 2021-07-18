package pretraitement_v8;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class IntegrateReducer extends Reducer<Text, Text, Text, Text> {
    
    private double keywordIDF = 0.0d;
    private Text value = new Text();
    private Text key2 = new Text();
    
    int nbrMots = 0 ;
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
    	
    	 
        if (values == null) {
            return;
        }
        //System.out.println("\t\n   Entree IntegrateReducer  ====  "+key.toString());
        
        if (key.toString().split(":")[1].startsWith("!")) {
            keywordIDF = Double.parseDouble(values.iterator().next().toString());
            //System.out.println("\t\n IntegrateReducer::  IDF  ====  "+keywordIDF);

            return;
        }
        

        String TFS = values.iterator().next().toString();//System.out.println("\t\n IntegrateReducer::  TF  ====  "+TFS);

        double TF = Double.parseDouble(TFS); 
        double TFIDF = TF * keywordIDF;
        
        String [] str = key.toString().split(":");
        
        key2.set(str[0]);
        
        
        //int totalFileCount = values.;
        
        

         //int wordID = Math.abs((str[0].hashCode() * 127) % 112);
         //int wordID = Math.abs(str[0].hashCode());
        

       
         
        //if (TFIDF>0.01)
        //{
            //nbrMots++;
            value.set(TFIDF+"");
            context.write(key, value);
        //}
        
    }
    
    @Override
    public void cleanup(Context context) throws IOException,InterruptedException 
    {
    	//System.out.println("\n\n\n\t N = "+nbrMots+"\n\n\n");
    }
}

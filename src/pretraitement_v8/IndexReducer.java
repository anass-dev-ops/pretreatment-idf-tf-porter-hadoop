package pretraitement_v8;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class IndexReducer extends Reducer<Text, Text, Text, Text>{
	
	int count= 0;
	int idword=0;
	
	

	
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
		
		idword++;
		while(values.iterator().hasNext())
        {
			String [] line=values.iterator().next().toString().split(":");

			context.write(new Text(line[0]), new Text(idword+":"+line[1]));
        }
		
		
		
		 //System.out.println("==>>> WORD  = "+ key.toString()+"  ID = "+ idword);
		 
		/*
		
		 while(values.iterator().hasNext())
	        {
				String line=values.iterator().next().toString();
			
				//System.out.println("\n\n KEY YY === "+ key.toString());
				//System.out.println("TESTSTSTSTS  ===  "+line);
				
				String [] str = line.split(":");
				System.out.println("arr["+Integer.parseInt(str[0])+"] = "+Double.parseDouble(str[1]));
				arr[Integer.parseInt(str[0])] = Double.parseDouble(str[1]);

			 
	        }
		 String valueT="";
		 System.out.println("Document = "+key.toString());
		 for(int i=0;i<142;i++)
		    {
			 //String.join(":", arr[i], arr[i+1]);
				valueT+= arr[i]+" ";
		    }
		 
		
		count++;
	     //context.write(key, new Text(valueT));
	     context.write(key, new Text(count+""));*/

	}
	
	 @Override
	    public void cleanup(Context context) throws IOException,InterruptedException 
	    {
	    	System.out.println("\n\n\n\t N = "+idword+"\n\n\n");
	    	context.write(new Text("!nbrMots"), new Text(idword+""));
	    }
	
}

package pretraitement_v8;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PREPReducer2 extends Reducer<Text, Text, Text, Text>{
	
	public int DIM;
	public double arr[] ;
	private Text value2 = new Text();

    //int nbrMots;
    int idfile = 0;
    
	@Override
    public void setup(Context context) throws IOException{
		
	}
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {		
		if (key.toString().startsWith("!")) 
        {
			
        	String nbrM = values.iterator().next().toString();

        	DIM = Integer.parseInt(nbrM);
        	
        	return;
        }
		/*
		arr = new double[DIM];

		
		for(int i=0;i<DIM;i++)
	    {
			arr[i]=0.0;
	    }
		
		
		while(values.iterator().hasNext())
        {
			String [] line=values.iterator().next().toString().split(":");
			
			int id_mot = Integer.parseInt(line[0]);
			double tfidf = Double.parseDouble(line[1]);
			arr[id_mot-1]=tfidf;
			

        }
		
		

		
		
		StringBuffer  b= new StringBuffer();
		for(int i=0;i<DIM;i++)
	    {
			
			if (i == DIM - 1)
				b.append(arr[i]);
			else
				b.append(arr[i]+" ");
	    }
		
		
		value2.set(idfile+"\t"+b.toString());
		//context.write(key, value2);
				
		
		value2.set(b.toString());

		*/
		
	    String Valu="";

		while(values.iterator().hasNext()) {
			
			String line=values.iterator().next().toString();
			Valu += line+",";	
		}
		idfile++;

		//context.write(new Text(idfile+""), new Text(Valu));
		context.write(new Text(idfile+""), new Text(Valu.substring(0, Valu.length()-1)));



	}

}

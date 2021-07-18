package pretraitement_v8;




import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;




public class TFMapper extends Mapper<Object, Text, Text, Text>{

	private final Text one = new Text("1");
    private Text label = new Text();
    private int allWordCount ;
    private String fileName = "";
    private String fileName2 = "";

    
	public static String stopWords;
	private boolean caseSensitive = false;
	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

	//String textoutput;
	
    @Override
    protected void setup(Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        //recuperation le nom de fichier
        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
        String[] nameSegments = filePathString.split("/");
        fileName = nameSegments[nameSegments.length - 2] +"_"+ nameSegments[nameSegments.length - 1];
        
        //pour le stopwords
        Configuration config = context.getConfiguration();
		this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
        stopWords = "'tis,'twas,a,able,about,across,after,ain't,all,almost,also,am,among,an,and,any,are,aren't,as,at,be,because,been,but,by,can,can't,cannot,could,could've,couldn't,dear,did,didn't,do,does,doesn't,don't,either,else,ever,every,for,from,get,got,had,has,hasn't,have,he,he'd,he'll,he's,her,hers,him,his,how,how'd,how'll,how's,however,i,i'd,i'll,i'm,i've,if,in,into,is,isn't,it,it's,its,just,least,let,like,likely,may,me,might,might've,mightn't,most,must,must've,mustn't,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,shan't,she,she'd,she'll,she's,should,should've,shouldn't,since,so,some,than,that,that'll,that's,the,their,them,then,there,there's,these,they,they'd,they'll,they're,they've,this,tis,to,too,twas,us,wants,was,wasn't,we,we'd,we'll,we're,were,weren't,what,what'd,what's,when,when,when'd,when'll,when's,where,where'd,where'll,where's,which,while,who,who'd,who'll,who's,whom,why,why'd,why'll,why's,will,with,won't,would,would've,wouldn't,yet,you,you'd,you'll,you're,you've,your";

        //textoutput = "";
        
    }
	
	
    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
    	//StopWords
    	//calcul le nombre totale de mots sur le document.
    	//mettre dans le key le mot et sa document.
    	//mettre le un dans la valure pour calculer le nombre de mots dans le documts apres, par le reducer. 
    	
    	String[] str99 = value.toString().split(";;&");
		fileName2 = str99[0];

    	//System.out.println("Voilaaa : "+ str99[0]);
    	//System.out.println("Voilaaa : "+ str99[1]);

    	String line = str99[1];
    	//String line = value.toString();
		// If we need case insensitivity, then convert all the words to lower case
		if (!caseSensitive) {
			line = line.toLowerCase();
		}
		Text currentWord = new Text();
		//Matches all the words only and remove all numbers and special characters
		Pattern pattern = Pattern.compile("[^a-z]");
		allWordCount = 0;
		for (String word : WORD_BOUNDARY.split(line)) {
			Matcher m = pattern.matcher(word);
			// ignores all the spaces and words that have length less than 5 characters
			if (word.isEmpty() || word.length() < 5 ) {
				continue;
			}
			// removes all words with special character
			else if(m.find()) {
				continue;
			}//removes all the stop words
			else if(stopwordmatch(word))
				continue;
			
			Stemmer stm = new Stemmer();
            String stemp1 = word;
            char[] inchar = stemp1.toCharArray();
            for (char temp : inchar) {
                temp = Character.toLowerCase((char) temp);
                stm.add(temp);
            }
            stm.stem();
            word = stm.toString();
		

            //textoutput+=word+" ";
            
            
			currentWord = new Text(word);
			allWordCount++;
            label.set(String.join(":",  fileName2, currentWord.toString()));

			//System.out.println(label.toString()+" ; "+one.toString());
            context.write(label,one);
		} 

    	
        
        context.write(new Text(fileName2 + ":!"), new Text(String.valueOf(allWordCount)));

    	
    	/*
    	StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()) {
        	allWordCount++;
            label.set(String.join(":", tokenizer.nextToken(), fileName));
            
            context.write(label, one);
        }*/
        
    }
    
    @Override
    protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
    	
        //System.out.println("\n Key = "+"!:" + fileName + "   Value = " +allWordCount);
        //System.out.println(textoutput);

        //context.write(new Text("!:" + fileName2), new Text(String.valueOf(allWordCount)));
    }

    
  //function to check if the given word is a stop word and if so returns true else false
  		public static boolean stopwordmatch(String word) {
  			String [] sw =stopWords.split(",");
  			for (int i=0;i<sw.length;i++) {
  				if(word.equalsIgnoreCase(sw[i]))
  					return true;

  			}
  			return false;

  		}
   

	   
	   
}

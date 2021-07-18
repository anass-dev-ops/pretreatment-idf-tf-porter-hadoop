package pretraitement_v8;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main_pretraitement {


	private static String inputPath  = "/OneFile/";
    private static String outputPath = "/output_1/";
	public static void main(String[] args) throws Exception {
    	

		Main_pretraitement client = new Main_pretraitement();

        if (args.length == 2) {
            inputPath = args[0];
            outputPath = args[1];
        }
	     long startTime = System.currentTimeMillis();

		client.execute();
		
		long stopTime = System.currentTimeMillis();
	    long elapsedTime = stopTime - startTime;
	    System.out.println("============Temps d'excution==============\n\n\n\n\n\t\t"+elapsedTime+"ms\n\n\n\n\n=======================");
	}
	
	
	private void execute() throws Exception {
        String tmpTFPath = outputPath + "RES_TF";
        String tmpIDFPath = outputPath + "RES_IDF";
        String RES_TFIDFPath = outputPath + "RES_TFIDF";
        
        
        runTFJob(inputPath, tmpTFPath);
        runIDFJob(tmpTFPath, tmpIDFPath);
        runIntegrateJob(tmpTFPath, tmpIDFPath, RES_TFIDFPath);
        
        runPREPJob(RES_TFIDFPath,"/output_1/RES_PREP");
        runPREP2Job("/output_1/RES_PREP","/output_1/RES_PREP_2");

    }

	
	private int runTFJob(String inputPath, String outputPath) throws Exception {
        Configuration configuration = new Configuration();
        removeOutputFolder(configuration, outputPath);
        
        Job job = Job.getInstance(configuration);
        job.setJobName("TF-job");
        job.setJarByClass(Main_pretraitement.class);

        job.setMapperClass(TFMapper.class);
        job.setCombinerClass(TFCombiner.class);
        //job.setPartitionerClass(TFPartitioner.class);
        //job.setNumReduceTasks(getNumReduceTasks(configuration, inputPath));
        //job.setReducerClass(TFReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath+"/MyFile_3000.txt"));

        
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
	
	
	 private int runIDFJob(String inputPath, String outputPath) throws Exception {
		 
		 //System.out.println("\t\n TESTsssssssssssssttttttttt : "+inputPath+"    "+outputPath);
	        Configuration configuration = new Configuration();
	        removeOutputFolder(configuration, outputPath);
	        
	        Job job = Job.getInstance(configuration);
	        job.setJobName("IDF-job");
	        job.setJarByClass(IDFMapper.class);
	        
	        job.setMapperClass(IDFMapper.class);
	        job.setReducerClass(IDFReducer.class);
//	        job.setNumReduceTasks(getNumReduceTasks(configuration, inputPath));
	        
			//System.out.println("\t\n PATH PATH PATH pdpdpdpd "+String.valueOf(getNumReduceTasks(configuration, inputPath)));
	        job.setProfileParams(String.valueOf(getNumReduceTasks(configuration, inputPath)));

	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);

	        FileInputFormat.addInputPath(job, new Path(inputPath));
	        FileOutputFormat.setOutputPath(job, new Path(outputPath));
	        
	        return job.waitForCompletion(true) ? 0 : 1;
	    }
	 
	 
	
	 
	 private int runIntegrateJob (String inputTFPath, String inputIDFPath, String outputPath) throws Exception {
	        Configuration configuration = new Configuration();
	        removeOutputFolder(configuration, outputPath);
	        
	        Job job = Job.getInstance(configuration);
	        job.setJobName("Integrate-job");
	        job.setJarByClass(IntegrateMapper.class);
	        
	        job.setMapperClass(IntegrateMapper.class);
	        job.setReducerClass(IntegrateReducer.class);
	        //job.setReducerClass(IntegrateReducer2.class);
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        
	        FileInputFormat.addInputPath(job, new Path(inputTFPath));
	        FileInputFormat.addInputPath(job, new Path(inputIDFPath));
	        FileOutputFormat.setOutputPath(job, new Path(outputPath));
	        
	        return job.waitForCompletion(true) ? 0 : 1;
	    }
	 
	 
	 
	 private int runPREPJob (String inputTFIDFPath, String outputPath) throws Exception {
	        Configuration configuration = new Configuration();
	        removeOutputFolder(configuration, outputPath);
	        
	        Job job = Job.getInstance(configuration);
	        job.setJobName("Preparation-job");
	        job.setJarByClass(IndexMapper.class);
	        
	        job.setMapperClass(IndexMapper.class);
	        job.setReducerClass(IndexReducer.class);
	        //job.setReducerClass(PREPReducer2.class);

	        //job.setNumReduceTasks(2);
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        
	        FileInputFormat.addInputPath(job, new Path(inputTFIDFPath));
	        FileOutputFormat.setOutputPath(job, new Path(outputPath));
	        
	        return job.waitForCompletion(true) ? 0 : 1;
	    }
	 
	 
	 private int runPREP2Job (String inputTFIDFPath, String outputPath) throws Exception {
	        Configuration configuration = new Configuration();
	        removeOutputFolder(configuration, outputPath);
	        
	        Job job = Job.getInstance(configuration);
	        job.setJobName("Preparation2-job");
	        job.setJarByClass(IndexMapper.class);
	        
	        job.setMapperClass(PREPMapper2.class);
	        job.setReducerClass(PREPReducer2.class);
	        //job.setReducerClass(PREPReducer2.class);

	        //job.setNumReduceTasks(2);
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        
	        FileInputFormat.addInputPath(job, new Path(inputTFIDFPath));
	        FileOutputFormat.setOutputPath(job, new Path(outputPath));
	        
	        return job.waitForCompletion(true) ? 0 : 1;
	    }
	 
	 
	 
	 
	 
	
    private int getNumReduceTasks(Configuration configuration, String inputPath) throws Exception {
        FileSystem hdfs = FileSystem.get(configuration);
        FileStatus status[] = hdfs.listStatus(new Path(inputPath));
        return status.length;
    }
    
    public void removeOutputFolder(Configuration configuration, String outputPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(outputPath);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

}

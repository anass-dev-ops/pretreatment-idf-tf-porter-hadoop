package pretraitement_v8;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TFPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        String fileName = key.toString().split(":")[1];
        //System.out.println("\n\t tseeseseseses  sesses : "+numPartitions+"        b          "+Math.abs((fileName.hashCode() * 127) % numPartitions));
        return Math.abs((fileName.hashCode() * 127) % numPartitions);
    }
}

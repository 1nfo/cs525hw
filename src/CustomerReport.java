package org.apache.hadoop.hw;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CustomerReport {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, NullWritable>{
  	private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	StringTokenizer itr = new StringTokenizer(value.toString());
    	while (itr.hasMoreTokens()) {
			String str = itr.nextToken();
			String[] tuple = str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			int county = Integer.parseInt(tuple[3]);
			if (county > 1 && county < 7){
        		word.set(str);
        		context.write(word, NullWritable.get());
			}
      	}
    }
  }
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.println("Usage: CustomerReport <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    Job job = new Job(conf, "customer filter 2-6");
    job.setJarByClass(CustomerReport.class);

    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    //job.setReducerClass(IntSumReducer.class);

	//map-only
    job.setNumReduceTasks(0);


    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    
	FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

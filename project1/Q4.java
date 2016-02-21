package org.apache.hadoop.hw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import javax.lang.model.type.NullType;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;


public class Q4 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: Q4 <HDFS Transactions> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "Query 4");
        //DistributedCache.addCacheFile(new URI(args[0]), conf);
        job.setJarByClass(Q4.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(GroupingReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setNumReduceTasks(4);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class JoinMapper extends Mapper<Object, Text, IntWritable, Text> {
        private HashMap<Integer, Integer> customers = new HashMap<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            //Path path = DistributedCache.getLocalCacheFiles(context.getConfiguration())[0];
            //System.err.print(DistributedCache.getLocalCacheFiles(context.getConfiguration()).length);
            BufferedReader bf = new BufferedReader(new FileReader("/home/hadoop/Customers"));
            String line;
            while ((line = bf.readLine()) != null) {
                String[] strs = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                int id = Integer.parseInt(strs[0]);
                customers.put(id, Integer.parseInt(strs[3]));
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tuple = value.toString().split(",");
            IntWritable code = new IntWritable(customers.get(Integer.parseInt(tuple[1])));
            context.write(code, new Text(tuple[1] + "," + tuple[2]));
        }
    }

    public static class GroupingReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable code, Iterable<Text> pairs, Context context) throws IOException, InterruptedException {
            HashMap<Integer, NullType> customercounter = new HashMap<>();
            double min = 5000000, max = 0;
            for (Text pair : pairs) {
                String[] strs = pair.toString().split(",");
                int id = Integer.parseInt(strs[0]);
                double item = Double.parseDouble(strs[1]);
                if (!customercounter.containsKey(id)) {
                    customercounter.put(id, null);
                }
                min = min > item ? item : min;
                max = max < item ? item : max;
            }
            context.write(code, new Text(customercounter.keySet().size() + "\t" + min + "\t" + max));
        }
    }
}

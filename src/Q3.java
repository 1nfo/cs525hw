package org.apache.hadoop.hw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class Q3 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: Q3 <HDFS Customers> <HDFS Transactions> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "Q3 two tables");
        job.setJarByClass(Q3.class);
        //Can't set combiner
        //job.setCombinerClass(JoinReducer.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setNumReduceTasks(4);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinCMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinTMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class JoinCMapper extends Mapper<Object, Text, IntWritable, Text> {
        private Text content = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String[] str = itr.nextToken().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                content.set("C," + str[1] + "," + str[4]);
                IntWritable id = new IntWritable(Integer.parseInt(str[0]));
                context.write(id, content);
            }
        }
    }

    public static class JoinTMapper extends Mapper<Object, Text, IntWritable, Text> {
        private Text content = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String[] str = itr.nextToken().split(",");
                content.set("T," + str[2] + "," + str[3]);
                IntWritable id = new IntWritable(Integer.parseInt(str[1]));
                context.write(id, content);
            }
        }
    }

    public static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String info = "None";
            int count = 0;
            double sum = 0;
            int min = 11;
            for (Text val : values) {
                String[] strs = val.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                switch (strs[0]) {
                    case "C":
                        info = strs[1] + "\t" + strs[2];
                        break;
                    case "T":
                        count++;
                        sum += Double.parseDouble(strs[1]);
                        int tmp = Integer.parseInt(strs[2]);
                        if (min > tmp) min = tmp;
                        break;
                    default:
                        info = "!" + val.toString() + "-len-" + strs.length + "!";
                        break;
                }
            }
            result.set(info + "\t" + count + "\t" + sum + "\t" + min);
            context.write(key, result);
        }
    }
}

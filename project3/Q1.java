import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class Q1 {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: Q1 <HDFS P> <HDFS R> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "Q1");
        job.setJarByClass(Q1.class);
        job.setReducerClass(ReducerQ1.class);
        job.setNumReduceTasks(4);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperP.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperR.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true)? 0:1);
    }

    public static class MapperP extends Mapper<Object,Text,Text,Text>{
        private int blockHeight=100,blockWidth=100;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()){
                String next = itr.nextToken();
                String[] strs=next.split(",");
                int x=Integer.parseInt(strs[0]),y=Integer.parseInt(strs[1]);
                int blockY=(y-1)/blockHeight,blockX=(x-1)/blockWidth;
                Text outKey = new Text(Integer.toString(blockX)+","+Integer.toString(blockY));
                context.write(outKey,new Text(next));
            }
        }
    }

    public static class MapperR extends Mapper<Object,Text,Text,Text>{
        private int blockHeight=100, blockWidth=100, pointRange=10000;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()){
                String next = itr.nextToken();
                String[] strs = next.split(",");
                int x=Integer.parseInt(strs[1]),y=Integer.parseInt(strs[2]),
                        w=Integer.parseInt(strs[3]),h=Integer.parseInt(strs[4]);
                int blockY=(y-1)/blockHeight,blockX=(x-1)/blockWidth;
                int blockYY=(y+h-1)/blockHeight,blockXX=(x+w-1)/blockWidth;
                Text outKey = new Text(Integer.toString(blockX)+","+Integer.toString(blockY));
                Text outKeyX = new Text(Integer.toString(blockXX)+","+Integer.toString(blockY));
                Text outKeyY = new Text(Integer.toString(blockX)+","+Integer.toString(blockYY));
                Text outKeyXY = new Text(Integer.toString(blockXX)+","+Integer.toString(blockYY));
                context.write(outKey,new Text(next));
                if (blockXX>blockX && blockXX<pointRange/blockWidth){
                    Text outValue = new Text(strs[0]+","+Integer.toString((blockX+1)*blockWidth+1)+","+y+","
                            +Integer.toString(x+w-(blockX+1)*blockWidth-1)+","+Integer.toString(h));
                    context.write(outKeyX,outValue);
                }
                else if (blockYY>blockY && blockYY<pointRange/blockHeight){
                    Text outValue = new Text(strs[0]+","+x+","+Integer.toString((blockY+1)*blockHeight+1)+","
                            +Integer.toString(w)+","+Integer.toString(y+h-(blockY+1)*blockHeight-1));
                    context.write(outKeyY, outValue);
                }
                else if (blockXX>blockX && blockXX<pointRange/blockWidth && blockYY>blockY && blockYY>pointRange/blockHeight){
                    Text outValue = new Text(strs[0]+","+Integer.toString((blockX+1)*blockWidth+1)+","
                            +Integer.toString((blockY+1)*blockHeight+1)+","
                            +Integer.toString(x+w-(blockX+1)*blockWidth-1)
                            +Integer.toString(y+h-(blockY+1)*blockHeight-1));
                    context.write(outKeyXY, outValue);
                }
            }
        }
    }

    public static class Rtest extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String[]> P = new ArrayList<>(), R = new ArrayList<>();
            for (Text value:values){
                String[] strs = value.toString().split(",");
                if (strs.length==2) P.add(strs);
                else R.add(strs);
            }
            String[] s = key.toString().split(",");
            for(String[] r:R){
                for(String[] p:P){
                    context.write(new Text(s[0]+"|"+s[1]),
                            new Text(p[0]+","+p[1]+",r"+r[1]));
                }
            }
        }
    }
    public static class ReducerQ1 extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String[]> P = new ArrayList<>(), R = new ArrayList<>();
            for (Text value:values){
                String[] strs = value.toString().split(",");
                if (strs.length==2) P.add(strs);
                else R.add(strs);
            }
            for(String[] r:R){
                // top left corner
                int cornerX=Integer.parseInt(r[1]),cornerY=Integer.parseInt(r[2]),
                        w=Integer.parseInt(r[3]),h=Integer.parseInt(r[4]);
                for(String[] p:P){
                    int x=Integer.parseInt(p[0]),y=Integer.parseInt(p[1]);
                    if(cornerX<=x && x<=cornerX+w && cornerY<=y && y<=cornerY+h){
                        context.write(new Text(r[0]),new Text(p[0]+","+p[1]));
                    }
                }
            }
        }
    }
}

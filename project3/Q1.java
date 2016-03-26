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
        if (args.length < 3 || args.length > 5) {
            System.err.println("Usage: Q1 <HDFS P> <HDFS R> <HDFS output file> <x1>,<y1>,<x2>,<y2> <BLOCK_SIZE>");
            System.exit(2);
        }
        conf.set("window",args[3]);
        conf.set("BLOCK_SIZE",args[4]);

        Job job = new Job(conf, "Q1");
        job.setJarByClass(Q1.class);
        job.setReducerClass(ReducerQ1.class);
        job.setNumReduceTasks(100);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperP.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperR.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true)? 0:1);
    }

    public static boolean filter(int x, int y, String window){
        if ("".equals(window)) return true;
        String[] s = window.split(",");
        int x1=Integer.parseInt(s[0]);
        int y1=Integer.parseInt(s[1]);
        int x2=Integer.parseInt(s[2]);
        int y2=Integer.parseInt(s[3]);
        return x1<=x && x2>=x && y1<=y && y2>=y;
    }
    public static class MapperP extends Mapper<Object,Text,Text,Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int BLOCK_SIZE = Integer.parseInt(context.getConfiguration().get("BLOCK_SIZE"));
            StringTokenizer itr = new StringTokenizer(value.toString());
            String next = itr.nextToken();
            String[] strs=next.split(",");
            int x=Integer.parseInt(strs[0]),y=Integer.parseInt(strs[1]);

            if (!filter(x,y,context.getConfiguration().get("window"))) return;

            int blockX = (x/BLOCK_SIZE)*BLOCK_SIZE;
            int blockY = (y/BLOCK_SIZE)*BLOCK_SIZE;
            Text outKey = new Text(Integer.toString(blockX)+","+Integer.toString(blockY));
            context.write(outKey,new Text(next));
        }
    }

    public static class MapperR extends Mapper<Object,Text,Text,Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int BLOCK_SIZE = Integer.parseInt(context.getConfiguration().get("BLOCK_SIZE"));
            StringTokenizer itr = new StringTokenizer(value.toString());
            ArrayList<String> blist = new ArrayList<String>();

            String next = itr.nextToken();
            String[] strs = next.split(",");
            int x=Integer.parseInt(strs[1]),y=Integer.parseInt(strs[2]),
                    w=Integer.parseInt(strs[3]),h=Integer.parseInt(strs[4]);
            Rect rect = new Rect(strs[0],x,y, w, h);
            String[] s = context.getConfiguration().get("window").split(",");
            int x1=Integer.parseInt(s[0]);
            int y1=Integer.parseInt(s[1]);
            int x2=Integer.parseInt(s[2]);
            int y2=Integer.parseInt(s[3]);
            Window window = new Window(x1,y1,x2,y2);
            if(isRectinWindow(rect,window)){
                blist = getBlocksInRect(rect, BLOCK_SIZE);
            }
            for(String b:blist){
                context.write(new Text(b), new Text(next));
            }
        }

        public static boolean isRectinWindow(Rect rect, Window w){
    /* w totally includes rect */
            if(rect.x_lt>=w.x_lt && rect.y_lt>=w.y_lt
                    && rect.x_rb<=w.x_rb && rect.y_rb<=w.y_rb) return true;
    //        rect totally includes w
            if((rect.x_lt<=w.x_lt&&rect.y_lt<=w.y_lt)
                    && (rect.x_rb>=w.x_rb && rect.y_rb>=w.y_rb)) return true;
    //        top left corner of rect in w
            if((rect.x_lt>=w.x_lt&&rect.y_lt>=w.y_lt)
                    && (rect.x_lt<=w.x_rb&&rect.y_lt<=w.y_rb)) return true;
    //        top right corner of rect in w
            if(rect.x_rb>=w.x_lt && rect.x_rb<=w.x_rb
                    && rect.y_lt>=w.y_lt && rect.y_lt<=w.y_rb) return true;
    //        bottom left corner of rect in w
            if(rect.x_lt>=w.x_lt && rect.x_lt<=w.x_rb
                    && rect.y_rb>=w.y_lt && rect.y_rb<=w.y_rb) return true;
    //        bottom right corner of rect in w
            if(rect.x_rb>=w.x_lt && rect.x_rb<=w.x_rb
                    && rect.y_rb>=w.y_lt && rect.y_rb<=w.y_rb) return true;

            return false;
        }

        public static ArrayList<String> getBlocksInRect(Rect rect, int BLOCK_SIZE){
            ArrayList<String> list = new ArrayList<String>();
            int bx_s = 0; //the x of left of the start block
    //        X-Left
            if(rect.x_lt%BLOCK_SIZE != 0){
                if(rect.x_lt/BLOCK_SIZE > 0) bx_s = (rect.x_lt/BLOCK_SIZE)*BLOCK_SIZE;
                else bx_s = 0; // the first column
            }else{
                if(rect.x_lt/BLOCK_SIZE > 0) bx_s = rect.x_lt;
                else bx_s = 0;
            }
            int bx_e = 0; // the x of the right of the end block
    //        X-Right
            if(rect.x_rb%BLOCK_SIZE != 0){
                if (rect.x_rb/BLOCK_SIZE > 0) bx_e = (rect.x_rb/BLOCK_SIZE)*BLOCK_SIZE;
                else bx_e = BLOCK_SIZE; // the first row
            }else{
                if (rect.x_rb/BLOCK_SIZE > 0) bx_e = rect.x_rb;
                else bx_e = BLOCK_SIZE;
            }
            int by_t = 0;
    //        Y-Top // the y of the top block
            if(rect.y_lt%BLOCK_SIZE != 0){
                if((rect.y_lt/BLOCK_SIZE)>0) by_t = (rect.y_lt/BLOCK_SIZE)*BLOCK_SIZE;
                else by_t = 0;
            }else{
                if((rect.y_lt/BLOCK_SIZE)>0) by_t = rect.y_lt;
                else by_t = 0;
            }
            int by_b = 0; // the y of the bottom block
    //        Y-Bottom
            if(rect.y_rb%BLOCK_SIZE != 0){
                if(rect.y_rb/BLOCK_SIZE>0) by_b = (rect.y_rb/BLOCK_SIZE)*BLOCK_SIZE;
                else by_b = BLOCK_SIZE;
            }else{
                if(rect.y_rb/BLOCK_SIZE>0) by_b = rect.y_rb;
                else by_b = BLOCK_SIZE;
            }

    //        the top left is (bx_s, by_t), the right bottom is (bx_e, by_b)
            for(int i=0;i<((bx_e-bx_s)/BLOCK_SIZE);i++) {
                for(int j=0;j<=((by_b-by_t)/BLOCK_SIZE);j++) {
                    int b_x = bx_s + i*BLOCK_SIZE;
                    int b_y = by_t + j*BLOCK_SIZE;
                    list.add(b_x+","+b_y); //using top left point to represent this block
                }
            }
            return list;
        }
    }
    public static class ReducerQ1 extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String[]> P = new ArrayList(), R = new ArrayList();
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

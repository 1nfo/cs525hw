import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class Q3  {
    private static final String flag = "/_converged";

    public static int configJob(String jobName, String inpath, String seed, String outpath, int step)
            throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        if (step==1) DistributedCache.addCacheFile(new URI(seed), conf);
        else {
            FileStatus[] statuses = FileSystem.get(conf).listStatus(new Path(seed));
            for (FileStatus statuse : statuses) {
                String p = statuse.getPath().toString();
                String[] strs = p.split("/");
                if (strs[strs.length - 1].charAt(0) != '_')
                    DistributedCache.addCacheFile(new URI(p), conf);
            }
        }
        int reducerN = 1;
        if (step==-1) {
            reducerN=0;//last step map-only
            conf.setBoolean("last",true);
        }
        Job job = new Job(conf, jobName);
        job.setJarByClass(Q3.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setCombinerClass(Combiner.class);
        job.setNumReduceTasks(reducerN);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inpath));
        FileOutputFormat.setOutputPath(job, new Path(outpath));
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length!=4) {
            System.err.println("Usage: Q3 <HDFS input points> <Local input seeds> <HDFS output centers> <HDFS out Index> ");
            System.exit(2);
        }
        int status= run(args);
        System.exit(status);
    }

    public static double dist(double xs, double x, double ys, double y) {
        return Math.pow(xs - x, 2) + Math.pow(ys - y, 2);
    }

    public static int run(String[] args) throws Exception {
        int i, step, status, maxiter=6;
        String seeds;
        final String points = args[0], outseeds = args[2];
        String last = "";
        for (i = 1; i < maxiter + 1; i++) {
            if (i==1) {
                step=1;//first iteration
                seeds=args[1];
            }
            else {
                seeds=outseeds;
                step=0;//mid-iteration
            }
            status = configJob(Integer.toString(i), points, seeds + last, outseeds + Integer.toString(i),step);
            last = Integer.toString(i);
            if (status==1) return 1;
            if (FileSystem.get(new Configuration()).exists(new Path(  //converged
                    new Path(outseeds).getParent().toString()+flag))) break;
        }
        status=configJob(Integer.toString(-1), points, outseeds + last, args[3], -1);
        return status;
    }

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        private ArrayList<Double> Xs = new ArrayList<>(), Ys = new ArrayList<>();
        private static boolean last=false;
        public void setup(Context context) throws IOException {
            Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path cache : caches) {
                BufferedReader bf = new BufferedReader(new FileReader(cache.toString()));
                while (true) {
                    String line = bf.readLine();
                    if (line == null) break;
                    String[] strs = line.trim().split(",");
                    Xs.add(Double.parseDouble(strs[0]));
                    Ys.add(Double.parseDouble(strs[1]));
                }
            }
            //if converged mappers do not print 1 at the end of value
            last=context.getConfiguration().getBoolean("last",false);
        }
        public void map(Object k, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split(",");
            int minI = -1, x = Integer.parseInt(strs[0]), y = Integer.parseInt(strs[1]);
            double mindist = Double.MAX_VALUE;
            for (int i = 0; i < Xs.size(); i++) {
                double newdist = dist(Xs.get(i), x, Ys.get(i), y);
                if (mindist > newdist) {
                    minI = i;
                    mindist = newdist;
                }
            }
            if (last) context.write(new Text(Integer.toString(minI)),value);
            else context.write(new Text(Integer.toString(minI)+"," +Double.toString(Xs.get(minI))+","
                    +Double.toString(Ys.get(minI))), new Text(value.toString()+",1"));
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        private boolean isconverge=true;
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double x = 0, y = 0, c = 0;
            for (Text val : values) {
                String[] strs = val.toString().split(",");
                if (2==strs.length) c++;
                else c+=Integer.parseInt(strs[2]);
                x += Double.parseDouble(strs[0]);
                y += Double.parseDouble(strs[1]);
            }
            Double newX=x / c, newY=y / c;
            String curr = Double.toString(newX) + "," + Double.toString(newY);
            double prevX = Double.parseDouble(key.toString().split(",")[1]);
            double prevY = Double.parseDouble(key.toString().split(",")[2]);
            isconverge &= dist(prevX,newX,prevY,newY)<0.01;
            context.write(new Text(curr),new Text());
        }
        public void cleanup(Context context) throws IOException {
            if (isconverge) FileSystem.get(context.getConfiguration()).create(new Path(
                    new Path(context.getConfiguration().get("out")).getParent().toString()+flag));
        }
    }

    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double x = 0, y = 0;
            int c = 0;
            for (Text val : values) {
                String[] strs = val.toString().split(",");
                c+=Integer.parseInt(strs[2]);
                x += Double.parseDouble(strs[0]);
                y += Double.parseDouble(strs[1]);
            }
            String comb_res = Double.toString(x) + "," + Double.toString(y) + "," + Integer.toString(c);
            context.write(key, new Text(comb_res));
        }
    }
}

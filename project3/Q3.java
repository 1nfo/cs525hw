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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class Q3 implements Tool {
    public static int configJob(String jobName, String inpath, String seed, String outpath, int step)
            throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        if (step==1) DistributedCache.addCacheFile(new URI(seed), conf);
        else {
            FileStatus[] statuses = FileSystem.get(conf).listStatus(new Path(seed));
            for(int i=0;i<statuses.length;i++){
                String p = statuses[i].getPath().toString();
                String[] strs = p.split("/");
                if(strs[strs.length-1].charAt(0)!='_')
                    DistributedCache.addCacheFile(new URI(p), conf);
            }
        }
        int reducerN=1;
        if (step==-1) reducerN=0;
        Job job = new Job(conf, jobName);
        job.setJarByClass(Q3.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        //job.setCombinerClass();
        job.setNumReduceTasks(reducerN);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inpath));
        FileOutputFormat.setOutputPath(job, new Path(outpath));
        if (job.waitForCompletion(true)) {
            if ("Y".equals(conf.get("converged"))) return 2;//converged
            return 1;//finished but not yet converged
        }
        return 0;// not completed
    }

    public static void main(String[] args) throws Exception {
        if (args.length!=4) {
            System.err.println("Usage: Q3 <HDFS input points> <Local input seeds> <HDFS output centers> <HDFS out Index> ");
            System.exit(2);
        }
        int status= ToolRunner.run(new Configuration(),new Q3(),args);
        System.exit(status > 0 ? 0 : 1);
    }

    public static double dist(double xs, int x, double ys, double y) {
        return Math.sqrt(Math.pow(xs - x, 2) + Math.pow(ys - y, 2));
    }

    @Override
    public int run(String[] args) throws Exception {
        int i,step,status = 1, maxiter = 6;
        String seeds="";
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
            if (status != 1) break;
        }
        configJob(Integer.toString(i), points, seeds + last, args[3], -1);
        return 0;
    }
    @Override
    public void setConf(Configuration configuration) {}
    @Override
    public Configuration getConf() {return null;}

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        private ArrayList<Double> Xs = new ArrayList<>(), Ys = new ArrayList<>();
        public void setup(Context context) throws IOException {
            Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (int i=0;i<caches.length;i++) {
                String prev =caches[i].toString();
                BufferedReader bf = new BufferedReader(new FileReader(prev));
                while (true) {
                    String line = bf.readLine();
                    if (line == null) break;
                    String[] strs = line.trim().split(",");
                    Xs.add(Double.parseDouble(strs[0]));
                    Ys.add(Double.parseDouble(strs[1]));
                }
            }
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
            assert minI != -1;
            context.write(new Text(Integer.toString(minI)), value);
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double x = 0, y = 0, c = 0;
            for (Text val : values) {
                String[] strs = val.toString().split(",");
                if (2==strs.length) c++;
                else c+=Integer.parseInt(strs[2]);
                x += Integer.parseInt(strs[0]);
                y += Integer.parseInt(strs[1]);
            }
            String converged, curr = Double.toString(x / c) + "," + Double.toString(y / c);
            context.write(new Text(curr),new Text());
            if (key.toString().equals(curr)) converged = "Y";
            else converged = "N";
            context.getConfiguration().set("converged", converged);
        }
    }
}

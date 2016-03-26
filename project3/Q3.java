import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class Q3 {
    public static int configJob(String jobName, String inpath, String seed, String outpath)
            throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("seed",seed);
        Job job = new Job(conf, jobName);
        job.setJarByClass(Q3.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        //job.setCombinerClass();
        job.setNumReduceTasks(0);
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

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {
        if (args.length!=3) {
            System.err.println("Usage: Q3 <HDFS input points> <HDFS input seeds> <HDFS output files> ");
            System.exit(2);
        }
        int status = 1, maxiter = 1;
        final String points = args[0], seeds = args[1], outseeds = args[2];
        String last = "";
        for (int i = 1; i < maxiter + 1; i++) {
            status = configJob(Integer.toString(i), points, seeds + last, outseeds + Integer.toString(i));
            last = Integer.toString(i);
            if (status != 1) break;
        }
        System.exit(status > 0 ? 0 : 1);
    }

    public static double dist(double xs, int x, double ys, double y) {
        return Math.sqrt(Math.pow(xs - x, 2) + Math.pow(ys - y, 2));
    }

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        private ArrayList<Integer> Xs = new ArrayList<>(), Ys = new ArrayList<>();

        public void setup(Context context) throws IOException {
            String prev = context.getConfiguration().get("seed");
            FSDataInputStream in = FileSystem.get(context.getConfiguration()).open(new Path(prev));
            BufferedReader bf = new BufferedReader(new InputStreamReader(in));
            while (true) {
                String line = bf.readLine();
                if (line == null) break;
                String[] strs = line.split(",");
                Xs.add(Integer.parseInt(strs[0]));
                Ys.add(Integer.parseInt(strs[1]));
            }
            assert Xs.size() == Ys.size();
        }

        public void map(Object k, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split(",");
            int x = Integer.parseInt(strs[0]), y = Integer.parseInt(strs[1]);
            double minI = -1, mindist = Double.MAX_VALUE;
            for (int i = 0; i < Xs.size(); i++) {
                double newdist = dist(Xs.get(i), x, Ys.get(i), y);
                if (mindist > newdist) {
                    minI = i;
                    mindist = newdist;
                }
            }
            assert minI != -1;
            context.write(new Text(Double.toString(minI)), value);
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        private String prev, curr, converged;

        public void setup(Context context) {
            prev = context.getCurrentKey().toString();
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double x = 0, y = 0, c = 0;
            for (Text val : values) {
                String[] strs = val.toString().split(",");
                c++;
                x += Integer.parseInt(strs[0]);
                y += Integer.parseInt(strs[1]);
            }
            curr = Double.toString(x / c) + "," + Double.toString(y / c);
            context.write(new Text(curr),new Text());
            if (prev.equals(curr)) converged = "Y";
            else converged = "N";
            context.getConfiguration().set("converged", converged);
        }
    }
}

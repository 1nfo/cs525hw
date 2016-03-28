import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapTask;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Created by wangqian on 3/26/16.
 */
public class MyKMeans {
    public static Logger logger = LogManager.getLogger(MyKMeans.class);

    static int MAXIteration = 50;
    static double Threshold = 1.0;


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        if (args.length!=3) {
            logger.error("Usage: MyKMeans <HDFS input points> <HDFS init_center> <HDFS output>");
            System.exit(2);
        }
        String inputDataPath = args[0];
        String initPath = args[1];
        String output = args[2];

        int iter;
        for(iter=0;iter<=MAXIteration;iter++){
            logger.info("Starting the '" + iter + "' iteration of KMeans Algorithm!");
            if(!isConverge(output,iter)){
                startNewJob(inputDataPath,initPath,output,iter);
            }else{
                logger.info("The KMeans Algorithm converged!");
                break;
            }
        }
        logger.info("Total iteration number is:" + iter);
        System.exit(1);
    }

    public static boolean isConverge(String output, int iter) throws IOException {
        // if the flag of all the centroids are converged true, then stop
        boolean converge = false;
        int notConvergeNum = 0;
        if(iter >0) {
            output = output + (iter - 1);
            Configuration conf = new Configuration();
            FileStatus[] statuses = FileSystem.get(conf).listStatus(new Path(output));
            for (FileStatus statuse : statuses) {
                String p = statuse.getPath().toString();
                String[] strs = p.split("/");
                if (strs[strs.length - 1].charAt(0) != '_') {
                    Path centerFile = new Path(p);
                    String line;
                    FileSystem fs = FileSystem.get(centerFile.toUri(), conf);
                    FSDataInputStream in = fs.open(centerFile);
                    InputStreamReader isr = new InputStreamReader(in);
                    BufferedReader br = new BufferedReader(isr);
                    while ((line = br.readLine()) != null) {
                        String[] centers = line.split(",");
                        if (!Boolean.parseBoolean(centers[2])) {
                            notConvergeNum++;
                        }
                    }
                }
            }
            if(notConvergeNum == 0) converge = true;
            logger.info("Not Converge center numbers: " + notConvergeNum);
        }

        return converge;
    }

    public static boolean startNewJob(String input, String init, String output, int iter) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        if(iter > 1){ // delete except the last two output data
            String delPath = output + (iter-2);
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(delPath),true);
        }

        String centers = output + (iter-1); //using last output as the input of next iteration

        if(iter == 0){
            DistributedCache.addCacheFile(new URI(init),conf);
        }else{
            FileStatus[] statuses = FileSystem.get(conf).listStatus(new Path(centers));
            for (FileStatus statuse: statuses){
                String p = statuse.getPath().toString();
                String[] strs = p.split("/");
                if(strs[strs.length-1].charAt(0) != '_'){
                    DistributedCache.addCacheFile(new URI(p), conf);
                }
            }
        }

        Job job = new Job(conf, "kmeans");
        job.setJarByClass(MyKMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class);
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        output = output + iter;
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true);
    }

    public static boolean isStopIteration(int iter, String prePath, String current) throws IOException {
        Configuration conf = new Configuration();
        boolean stop = false;
        String[] paths = {prePath, current};
        ArrayList<ArrayList<String>> mylist = new ArrayList<ArrayList<String>>();

        for(int i=0;i<paths.length;i++) {
            FileStatus[] statuses = FileSystem.get(conf).listStatus(new Path(paths[i]));
            ArrayList<String> pathList = new ArrayList<String>();
            for (FileStatus statuse : statuses) {
                String p = statuse.getPath().toString();
                String[] strs = p.split("/");
                if (strs[strs.length - 1].charAt(0) != '_') {
                    Path centerFile = new Path(prePath);
                    String line;
                    FileSystem fs = FileSystem.get(centerFile.toUri(), conf);
                    FSDataInputStream in = fs.open(centerFile);
                    InputStreamReader isr = new InputStreamReader(in);
                    BufferedReader br = new BufferedReader(isr);
                    while ((line = br.readLine()) != null) {
                        pathList.add(line);
                    }
                }
            }
            mylist.add(pathList);
        }

        assert mylist.size() == 2;
        ArrayList<String> list1 = mylist.get(0); //the last centers
        ArrayList<String> list2 = mylist.get(1); //the current centers

        for(int i=0;i<list1.size();i++){
            String line1 = list1.get(i);
            String line2 = list2.get(i);
            String[] str1 = line1.split(",");
            String[] str2 = line2.split(",");
            double errorX = Math.pow((Double.parseDouble(str1[0])-Double.parseDouble(str2[0])),2);
            double errorY = Math.pow((Double.parseDouble(str1[1])-Double.parseDouble(str2[1])),2);
            if(Math.sqrt(errorX + errorY)<Threshold){
                stop = true;
            }
        }
//        fs.delete(new Path(prePath));
        return stop;
    }

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        static ArrayList<Double> Xs = new ArrayList<Double>(), Ys = new ArrayList<Double>();
        public void setup(Context context) throws IOException {
            Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for(Path cache : caches){
                BufferedReader bf = new BufferedReader(new FileReader(cache.toString()));
                String line = null;
                while((line = bf.readLine()) != null){
                    String[] point = line.split(",");
                    Xs.add(Double.parseDouble(point[0]));
                    Ys.add(Double.parseDouble(point[1]));
                }
            }
            assert (Xs.size() == Ys.size()) && Xs.size() > 0;
        }

        public void map(Object k, Text value, Context context) throws IOException, InterruptedException {
            String[] point = value.toString().split(",");
            int x = Integer.parseInt(point[0]), y = Integer.parseInt(point[1]);
            int min_index = -1;
            double min_error = Double.MAX_VALUE;
            for(int i=0;i<Xs.size();i++){
                double error = Math.sqrt(Math.pow((x-Xs.get(i)),2) + Math.pow((y-Ys.get(i)),2));
                if(error < min_error){
                    min_index = i;
                    min_error = error;
                }
            }
            String key = Xs.get(min_index) + "," + Ys.get(min_index);
            String val = value.toString() + "," + 1;
            context.write(new Text(key), new Text(val));
        }
    }

    public static class KMeansCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double x = 0, y = 0;
            int count = 0;
            for(Text value: values){
                String[] point = value.toString().split(",");
                x += Double.parseDouble(point[0]);
                y += Double.parseDouble(point[1]);
                count += Double.parseDouble(point[2]);
            }
            String outString = Double.toString(x) + "," + Double.toString(y) + "," + count;
            context.write(key, new Text(outString));
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double x = 0, y = 0;
            int count = 0;
            for(Text value: values){
                String[] strs = value.toString().split(",");
                x += Double.parseDouble(strs[0]);
                y += Double.parseDouble(strs[1]);
                count += Integer.parseInt(strs[2]);
            }
            x /= count; y /= count;
            String[] strs = key.toString().split(",");
            double preX = Double.parseDouble(strs[0]);
            double preY = Double.parseDouble(strs[1]);
            boolean converge = false;
            double error = Math.sqrt(Math.pow(x-preX,2)+Math.pow(y-preY,2));
            if(error <= Threshold) converge = true;
            context.write(new Text(x + "," + y), new Text("," + converge));
        }
    }
}

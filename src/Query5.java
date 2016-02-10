import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by wangqian on 2/5/16.
 *
 * We used one Mapper-Reducer pair to solve the Query5.
 * In mapper part, we used DistributedCache library to read the Customers information into mapper using setup function.
 * Then join the customer and transaction using the key of custID. Next write the key is custName, value is the transID.
 * In reducer part, we aggregate the transaction number for every customer. Then we override the cleanup function in reducer,
 * after comparation the transaction number for every customer then get the customers who have the least transaction number.
 */
public class Query5  extends Configured implements Tool {

    public static class Query5Mapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private  static HashMap<String,String> CustMap = new HashMap<String,String>();
        private BufferedReader brReader;
        private String custName = "";
        private Text txtMapOutputKey = new Text("");
        private Text txtMapoutputValue = new Text("");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{

            Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());

            for (Path eachPath: cacheFilesLocal) {
                if (eachPath.getName().toString().trim().equals("Customers")){
                    loadCustomerHashMap(eachPath,context);
                }
            }
        }

        private void loadCustomerHashMap(Path filePath, Context context) throws IOException {
            String strLineRead = "";

            try {
                brReader = new BufferedReader(new FileReader(filePath.toString()));
                while ((strLineRead = brReader.readLine()) != null){
                    String[] custArray = strLineRead.split(",");
                    CustMap.put(custArray[0].trim(), custArray[1].trim()); //key: ID, value: Name
                }
            } catch (FileNotFoundException e){
                e.printStackTrace();
            } catch (IOException e){
                e.printStackTrace();
            } finally{
                if (brReader != null){
                    brReader.close();
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            if (value.toString().length() > 0){
                String[] trans = value.toString().split(",");

                try {
                    custName = CustMap.get(trans[1].trim());
                } finally {
                    custName = ((custName == null || custName.equals(""))
                            ? "NOT-Found" : custName);
                }

                txtMapOutputKey.set(custName);              //CustomerName
                txtMapoutputValue.set(trans[0].trim());     //TransID
            }
            context.write(txtMapOutputKey, txtMapoutputValue);
            custName = "";
        }
    }

    public static class Query5Reducer extends
            Reducer<Text,Text,IntWritable,Text> {

        int number = Integer.MAX_VALUE;
        ArrayList<String> custNames = new ArrayList<String>();
        String value = "";

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            if (custNames.size()>0){
                for (String name: custNames){
                    value = value + "," + name;
                }
            }
            System.out.println("Customer List Size is: " + custNames.size());
            context.write(new IntWritable(number), new Text(value)); //transNum, CustName List
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, //key: custName, values:transIDs
                           Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text t : values){
                count ++;
            }
            if (count < number){
                custNames = new ArrayList<String>();
                custNames.add(key.toString());
                number = count;
            } else if (count == number){
                custNames.add(key.toString());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Two parameters are required: <input dir> <output dir> \n");
            return -1;
        }

        Job job = new Job(getConf());
        Configuration conf = job.getConfiguration();
        job.setJobName("Map-side join with text look file in DCache");
        DistributedCache.addCacheFile(new URI("/user/hadoop/data/Customers"), conf);
        job.setJarByClass(Query5.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Query5Mapper.class);
        job.setReducerClass(Query5Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),new Query5(), args);
        System.exit(exitCode);
    }
}

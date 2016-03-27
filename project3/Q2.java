import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class CustomRecordReader extends RecordReader<LongWritable, Text> {
    private LongWritable key = null;
    private Text value = null;
    private BufferedReader bf;
    private int pos=0;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        bf = new BufferedReader(new InputStreamReader(fs.open(file)));
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        int c, counter=0;
        String line="";
        while(true){
            c=bf.read();
            if (c==-1) {
                key=null;
                value=null;
                return false;
            }
            if (c<33) continue;//omit spaces
            if (c==123) counter++;
            else if (c==125) counter--;
            else if (counter!=0) line+=(char)c;//only collect between "{" and "}"
            else continue;
            if (counter==0) break;
        }
        String newline=help(line,"CustomerID")+","+help(line,"Name")+","
                +help(line,"Address")+","+help(line,"Salary")+","+help(line,"Gender");
        if (key==null) key = new LongWritable();
        if (value==null) value = new Text();
        key.set(++pos);
        value.set(newline);
        return true;
    }

    String help(String line, String pattern){
        Matcher m =Pattern.compile("(?<="+pattern+":)[^,]*").matcher(line);
        if (m.find()) return m.group();
        else return "XXX";//should not happen
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0.0f;
    }

    @Override
    public void close() throws IOException {
        if (bf != null) bf.close();
    }
}
class CustomInputFormat extends FileInputFormat<LongWritable,Text> {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new CustomRecordReader();
    }
}
public class Q2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2 ) {
            System.err.println("Usage: Q2 <HDFS input file> <HDFS output file> ");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Read Json");
        job.setJarByClass(Q2.class);
        job.setMapperClass(MapperQ2.class);
        job.setReducerClass(ReducerQ2.class);
        job.setNumReduceTasks(4);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(CustomInputFormat.class);
        CustomInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)? 0:1);
    }

    public static class MapperQ2 extends Mapper<LongWritable,Text,Text,LongWritable>{
        public void map(LongWritable k, Text val, Context context) throws IOException, InterruptedException {
            String[] strs = val.toString().split(",");;
            context.write(new Text(strs[3]+","+strs[4]),new LongWritable(1));
        }
    }

    public static class ReducerQ2 extends Reducer<Text,LongWritable,Text,LongWritable>{
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for (LongWritable val:values){
                count+=val.get();
            }
            context.write(key,new LongWritable(count));
        }
    }
}

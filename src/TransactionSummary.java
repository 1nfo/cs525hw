/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hadoop.hw;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TransactionSummary {
    public static class PairWritable implements Writable {
        int count;
        double sum;

        public PairWritable(){}

        public PairWritable(int count, double sum) {
            this.count = count;
            this.sum = sum;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(count);
            dataOutput.writeDouble(sum);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            count = dataInput.readInt();
            sum = dataInput.readDouble();
        }

        @Override
        public String toString() {
            return this.count + "\t" + this.sum;
        }
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, PairWritable> {

        private IntWritable id = new IntWritable();
        private PairWritable pair;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String[] tuple = itr.nextToken().split(",");
                id.set(Integer.parseInt(tuple[1]));
                pair = new PairWritable(1, Double.parseDouble(tuple[2]));
                context.write(id, pair);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable, PairWritable, IntWritable, PairWritable> {

        private PairWritable result;

        public void reduce(IntWritable key, Iterable<PairWritable> values,
                           Context context) throws IOException, InterruptedException {
            int count = 0, sum = 0;
            for (PairWritable val : values) {
                sum += val.sum;
                count += val.count;
            }
            result = new PairWritable(count, sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: TransactionSummary <HDFS input file> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "Transaction Summary");
        job.setJarByClass(TransactionSummary.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setNumReduceTasks(4);
        job.setOutputValueClass(PairWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

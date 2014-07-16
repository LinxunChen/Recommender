package org.lynnc.myhadoop.recommend;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.lynnc.myhadoop.hdfs.HdfsOperator;


import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.util.Iterator;
import java.util.Map;

public class Step2 {

    public static class Step2_UserCooccurrenceMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text k = new Text();
        private IntWritable v = new IntWritable(1);

        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            for (int i = 1; i < tokens.length; i++) {
                String userID = tokens[i].split(":")[0];
                for (int j = 1; j < tokens.length; j++) {
                    String userID2 = tokens[j].split(":")[0];
                    k.set(userID + ":" + userID2);
                    context.write(k, v);
                }
            }
        }
    }

    public static class Step2_UserCooccurrenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable v = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            while (values.iterator().hasNext()) {
                sum += values.iterator().next().get();
            }
            v.set(sum);

            context.write(key, v);
        }
    }

    public static void run(Map<String, String> path) throws FileSystemNotFoundException, IOException, InterruptedException, ClassNotFoundException {

        JobConf conf = Recommend.config();

        String input = path.get("Step2Input");
        String output = path.get("Step2Output");

        HdfsOperator hdfs = new HdfsOperator(Recommend.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf);
        job.setJarByClass(Step2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Step2.Step2_UserCooccurrenceMapper.class);
        job.setReducerClass(Step2.Step2_UserCooccurrenceReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}

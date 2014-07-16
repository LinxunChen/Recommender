package org.lynnc.myhadoop.recommend;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.lynnc.myhadoop.hdfs.HdfsOperator;

import java.io.IOException;
import java.util.Map;

public class Step3 {

    public static class Step3_UserVectorMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable k = new IntWritable();
        private Text v = new Text();

        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            int itemID = Integer.parseInt(tokens[0]);
            for (int i = 1; i < tokens.length; i++) {
                k.set(Integer.parseInt(tokens[i].split(":")[0]));
                v.set(itemID + ":" + tokens[i].split(":")[1]);
                context.write(k, v);
            }
        }
    }

    public static void run (Map<String, String> path) throws IOException,InterruptedException, ClassNotFoundException {
        JobConf conf = Recommend.config();

        String input = path.get("Step3Input");
        String output = path.get("Step3Output");

        HdfsOperator hdfs = new HdfsOperator(Recommend.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step3.Step3_UserVectorMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}

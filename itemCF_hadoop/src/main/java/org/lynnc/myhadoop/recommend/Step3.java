package org.lynnc.myhadoop.recommend;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.lynnc.myhadoop.hdfs.HdfsOperator;

/* Step3负责对Step1输出的用户对物品的评分矩阵进行倒排，转换为“itemID    userID:评分”这种形式 */
public class Step3 {

    /* map过程输入的是userID "itemID1:评分,itemID2:评分,itemID3:评分..."; 输出的key是"itemID1"，value是"userID:评分"，key是"itemID2"，value是"userID:评分"（示例） */
    public static class Step3_ItemVectorMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();

        @Override
        public void map(LongWritable key, Text values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            for (int i = 1; i < tokens.length; i++) {
                String[] vector = tokens[i].split(":");
                int itemID = Integer.parseInt(vector[0]);
                String pref = vector[1];

                k.set(itemID);
                v.set(tokens[0] + ":" + pref);
                output.collect(k, v);
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException {
        JobConf conf = Recommend.config();

        String input = path.get("Step3Input");
        String output = path.get("Step3Output");

        HdfsOperator hdfs = new HdfsOperator(Recommend.HDFS, conf);
        hdfs.rmr(output);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Step3_ItemVectorMapper.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }

//    public static class Step32_CooccurrenceColumnWrapperMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
//        private final static Text k = new Text();
//        private final static IntWritable v = new IntWritable();
//
//        @Override
//        public void map(LongWritable key, Text values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
//            String[] tokens = Recommend.DELIMITER.split(values.toString());
//            k.set(tokens[0]);
//            v.set(Integer.parseInt(tokens[1]));
//            output.collect(k, v);
//        }
//    }
//
//    public static void run2(Map<String, String> path) throws IOException {
//        JobConf conf = Recommend.config();
//
//        String input = path.get("Step3Input2");
//        String output = path.get("Step3Output2");
//
//        HdfsOperation hdfs = new HdfsOperation(Recommend.HDFS, conf);
//        hdfs.rmr(output);
//
//        conf.setOutputKeyClass(Text.class);
//        conf.setOutputValueClass(IntWritable.class);
//
//        conf.setMapperClass(Step32_CooccurrenceColumnWrapperMapper.class);
//
//        conf.setInputFormat(TextInputFormat.class);
//        conf.setOutputFormat(TextOutputFormat.class);
//
//        FileInputFormat.setInputPaths(conf, new Path(input));
//        FileOutputFormat.setOutputPath(conf, new Path(output));
//
//        RunningJob job = JobClient.runJob(conf);
//        while (!job.isComplete()) {
//            job.waitForCompletion();
//        }
//    }

}

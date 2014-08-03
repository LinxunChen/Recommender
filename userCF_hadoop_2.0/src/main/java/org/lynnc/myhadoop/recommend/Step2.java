package org.lynnc.myhadoop.recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.lynnc.myhadoop.hdfs.HdfsOperator;

import java.io.IOException;
import java.util.Map;

/* Step2 map负责对Step1输出的不同用户对同一物品的评分矩阵进行倒排，转换为“userID    itemID:评分”这种形式
* mapreduce一起负责生成每个用户对其所有评价物品的评分矩阵，即“userID   itemID1:评分，itemID2:评分，itemID3:评分”，为记录用户的行为物品做准备*/
public class Step2 {

    /* map过程输入的是”itemID  userID1:评分,userID2:评分,userID3:评分..."; 输出的key是"userID1"，value是"itemID:评分"，key是"userID2"，value是"itemID:评分"（示例） */
    public static class Step2_UserVectorMapper extends Mapper<Object, Text, IntWritable, Text> {
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

    /* reduce过程输入的key是"userID"，value是;"itemID1:评分"，"itemID2:评分"，"itemID3:评分"， 输出的key是"userID"，value是"itemID1:评分，itemID2:评分，itemID3:评分" （示例） */
    public static class Step2_UserVectorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private Text v = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text line : values) {
                sb.append("," + line);
            }
            v.set(sb.toString().replaceFirst(",", ""));
            context.write(key, v);
        }
    }

    public static void run (Map<String, String> path) throws IOException,InterruptedException, ClassNotFoundException {
        Configuration conf = Recommend.config();

        String input = path.get("Step2Input");
        String output1 = path.get("Step2Output1");
        String output2 = path.get("Step2Output2");

        HdfsOperator hdfs = new HdfsOperator(Recommend.HDFS, conf);
        hdfs.rmr(output1);
        hdfs.rmr(output2);

        Job job = new Job(conf, "Step2");

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step2.Step2_UserVectorMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output1));

        job.waitForCompletion(true);

        Job job1 = new Job(conf);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(Step2.Step2_UserVectorMapper.class);
        job1.setReducerClass(Step2.Step2_UserVectorReducer.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output2));

        job1.waitForCompletion(true);
    }
}

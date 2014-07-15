package org.lynnc.myhadoop.recommend;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.lynnc.myhadoop.hdfs.HdfsOperator;

/* Step1 负责生成每个用户对物品的评分矩阵，即“userID   itemID:评分” */
public class Step1 {

    /* map过程输入的是文件内容; 输出的key为userID，value为"itemID:评分" */
    public static class Step1_UserVectorPreMapper extends MapReduceBase implements Mapper<Object, Text, IntWritable, Text> {
        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());
//            int itemID = Integer.parseInt(tokens[1]);
//            String userID = tokens[0];
//            String pref = tokens[2];
//            k.set(itemID);
//            v.set(userID + ":" + pref);
            int userID = Integer.parseInt(tokens[0]);
            String itemID = tokens[1];
            String pref = tokens[2];
            k.set(userID);
            v.set(itemID + ":" + pref);
            output.collect(k, v);
        }
    }

    /* reduce过程输入的key为userID，value为"itemID1:评分"、"itemID2:评分"、"itemID3:评分"...; 输出的key为userID，value为"itemID1:评分,itemID2:评分,itemID3:评分..." */
    public static class Step1_UserVectorReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
        private final static Text v = new Text();

        @Override
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            StringBuilder sb = new StringBuilder();
            while (values.hasNext()) {
                sb.append("," + values.next());
            }
            v.set(sb.toString().replaceFirst(",", ""));
            output.collect(key, v);
        }
    }

    public static Map<String, List> userItem = new HashMap<String, List>();//定义一个静态的hashmap来记录每个用户对哪些物品有过评价行为

    public static void run(Map<String, String> path) throws IOException {
        /* userItem记录每个用户对哪些物品有过评价行为 */
        BufferedReader br = new BufferedReader(new FileReader(path.get("data")));
        String temp = null;
        while((temp=br.readLine())!=null) {
            String[] tokens = Recommend.DELIMITER.split(temp);
            if (userItem.containsKey(tokens[0])) {
                userItem.get(tokens[0]).add(tokens[1]);
            }
            else{
                List<String> itemList = new ArrayList();
                userItem.put(tokens[0], itemList);
                itemList.add(tokens[1]);
            }
        }

        JobConf conf = Recommend.config();

        String input = path.get("Step1Input");
        String output = path.get("Step1Output");

        HdfsOperator hdfs = new HdfsOperator(Recommend.HDFS, conf);
        hdfs.rmr(input);
        hdfs.mkdirs(input);
        hdfs.copyFile(path.get("data"), input);

        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Step1_UserVectorPreMapper.class);
        conf.setCombinerClass(Step1_UserVectorReducer.class);
        conf.setReducerClass(Step1_UserVectorReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }

}

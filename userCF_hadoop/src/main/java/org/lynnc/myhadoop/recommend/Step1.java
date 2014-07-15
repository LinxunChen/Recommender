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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.util.*;

public class Step1 {

    public static class Step1_ItemVectorMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable k = new IntWritable();
        private Text v = new Text();

        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException{
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            int itemID = Integer.parseInt(tokens[1]);
            String userID = tokens[0];
            String pref = tokens[2];

            k.set(itemID);
            v.set(userID + ":" + pref);

            context.write(k, v);
        }
    }

    public static class Step1_ItemVectorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private Text v = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            while (values.iterator().hasNext()) {
                sb.append("," + values.iterator().next());
            }
            v.set(sb.toString().replaceFirst(",", ""));

            context.write(key, v);
        }
    }

    public static Map<String, List> userItem = new HashMap<String, List>();//定义一个静态的hashmap来记录每个用户对哪些物品有过评价行为

    public static void run(Map<String, String> path) throws FileSystemNotFoundException, IOException, InterruptedException, ClassNotFoundException {

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

        Job job = new Job(conf);
        job.setJarByClass(Step1.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step1.Step1_ItemVectorMapper.class);
        job.setReducerClass(Step1.Step1_ItemVectorReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}

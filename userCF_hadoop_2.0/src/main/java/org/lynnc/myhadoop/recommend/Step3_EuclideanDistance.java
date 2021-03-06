package org.lynnc.myhadoop.recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.nio.file.FileSystemNotFoundException;
import java.util.Map;

/* Step3 负责生成用户的相似度矩阵（基于欧氏距离） */
public class Step3_EuclideanDistance {

    /* map过程输入的是“itemID  userID1:评分,userID2:评分,userID3:评分..."; 输出的key是"userID1:userID2"...，value是"itemID, diff" */
    public static class Step3_UserSimilarityMatrixMapper extends Mapper<Object, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            for (int i = 1; i < tokens.length; i++) {
                String userID = tokens[i].split(":")[0];
                double pref1 = Double.parseDouble(tokens[i].split(":")[1]);
                for (int j = 1; j < tokens.length; j++) {
                    String userID2 = tokens[j].split(":")[0];
                    double pref2 = Double.parseDouble(tokens[j].split(":")[1]);
                    String diff = new Double(Math.pow(pref1-pref2, 2)).toString();
                    k.set(userID + ":" + userID2);
                    v.set(diff);
                    context.write(k, v);
                }
            }
        }
    }

    /* reduce过程输入的key是"userID1:userID2"，value是"diff"，"diff"...; 输出的key是"userID1:userID2"，value是“similarity” （示例） */
    public static class Step3_UserSimilarityMatrixReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable v = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum_square = 0;
            double sim = 0;
            int count = 0;

            while (values.iterator().hasNext()) {
                count++;
                sum_square += Double.parseDouble(values.iterator().next().toString());
            }

            if (count > 2) {    //两个用户有2个以上的相同操作物品，才计算其相似度
                sim = 1/(1+Math.sqrt(sum_square));
                v.set(sim);
                context.write(key, v);
            }
        }
    }

    public static void run(Map<String, String> path) throws FileSystemNotFoundException, IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = Recommend.config();

        String input = path.get("Step3Input");
        String output = path.get("Step3Output");

        HdfsOperator hdfs = new HdfsOperator(Recommend.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf, "Step3");
        job.setJarByClass(Step3_EuclideanDistance.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step3_EuclideanDistance.Step3_UserSimilarityMatrixMapper.class);
        job.setReducerClass(Step3_EuclideanDistance.Step3_UserSimilarityMatrixReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}

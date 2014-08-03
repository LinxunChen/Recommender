package org.lynnc.myhadoop.recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.lynnc.myhadoop.hdfs.HdfsOperator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/* Step4负责从单个itemID的视角下，预测对该物品有评价的用户给所有itemID的评分 */
public class Step4 {

    /* 倒排后的评分矩阵：map过程输入的是"userID  itemID:评分"; 输出的key是"userID"，value是“B:itemID,评分”
    *  相似度矩阵：map过程输入的是"userID1:userID2 相似度“; 输出的key是"userID"，value是“A:userID,相似度”*/
    public static class Step4_PartialMultiplyMapper extends Mapper<Object, Text, Text, Text> {
        private String flag;// 标记，以区分相似度矩阵or评分矩阵
        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// 判断读的数据集
        }

        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());

            if (flag.equals("step3")) {// flag为step2则输入的为相似度矩阵，接下来标记为A
                String[] v1 = tokens[0].split(":");
                String userID1 = v1[0];
                String userID2 = v1[1];
                String sim = tokens[1];

                k.set(userID1);
                v.set("A:" + userID2 + "," + sim);
                context.write(k, v);
            }
            else if (flag.equals("step2_1")) {// flag为step3_1则输入的为评分矩阵，接下来标记为B
                String[] v2 = tokens[1].split(":");
                String userID = tokens[0];
                String itemID = v2[0];
                String pref = v2[1];

                k.set(userID);
                v.set("B:" + itemID + "," + pref);
                context.write(k, v);
            }
        }
    }

    /* 倒排后的评分矩阵：reduce过程输入的key是"userID"，value是“B:itemID1,评分”、“B:itemID2,评分”、“B:itemID3,评分”...;
    *  相似度矩阵：reduce过程输入的key是"userID"，value是“A:userID1,相似度”、“A:userID2,相似度”、“A:userID3,相似度”...;
    *  reduce输出的key是"userID"，value是”itemID,预测评分,相似度“*/
    public static class Step4_AggregateReducer extends Reducer<Text, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, String> mapA = new HashMap();//key:userID,value:相似度
            Map<String, String> mapB = new HashMap();//key:itemID,value:评分

            for (Text line : values) {
                String val = line.toString();
                if (val.startsWith("A:")) {
                    String[] kv = Recommend.DELIMITER.split(val.substring(2));
                    if (!kv[0].equals(key.toString())) {  //过滤掉自己对自己的推荐
                        mapA.put(kv[0], kv[1]);
                    }
                } else if (val.startsWith("B:")) {
                    String[] kv = Recommend.DELIMITER.split(val.substring(2));
                    mapB.put(kv[0], kv[1]);
                }
            }

            double result = 0;
            Iterator<String> iter = mapA.keySet().iterator();

            while (iter.hasNext()) {
                String similarUser = iter.next();// 遍历与此userID（也就是key）具有一定相似度的userID（自己也包含了）
                double similarity = Double.parseDouble(mapA.get(similarUser));
                Iterator<String> iterb = mapB.keySet().iterator();
                while (iterb.hasNext()) {
                    String prefItem = iterb.next();// itemID
                    double pref = Double.parseDouble(mapB.get(prefItem));
                    result = similarity * pref;// 矩阵乘法相乘计算

                    k.set(similarUser);
                    v.set(prefItem + "," + result + "," + similarity);
                    context.write(k, v);
                }
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = Recommend.config();

        String input1 = path.get("Step4Input1");
        String input2 = path.get("Step4Input2");
        String output = path.get("Step4Output");

        HdfsOperator hdfs = new HdfsOperator(Recommend.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf, "Step4");
        job.setJarByClass(Step4.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4.Step4_PartialMultiplyMapper.class);
        job.setReducerClass(Step4.Step4_AggregateReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}

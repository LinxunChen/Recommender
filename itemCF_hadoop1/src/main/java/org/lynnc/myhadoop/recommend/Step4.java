package org.lynnc.myhadoop.recommend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.lynnc.myhadoop.hdfs.HdfsOperator;

/* Step4负责从单个itemID的视角下，预测对该物品有评价的用户给所有itemID的评分 */
public class Step4 {

    /* 倒排后的评分矩阵：map过程输入的是"itemID  userID:评分"; 输出的key是"itemID"，value是“B:userID,评分”
    *  相似度矩阵：map过程输入的是"itemID1:itemID2 相似度“; 输出的key是"itemID"，value是“A:itemID,相似度”*/
    public static class Step4_PartialMultiplyMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String flag;// 标记，以区分相似度矩阵or评分矩阵

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// 判断读的数据集

        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());

            if (flag.equals("step2")) {// flag为step2则输入的为相似度矩阵，接下来标记为A
                String[] v1 = tokens[0].split(":");
                String itemID1 = v1[0];
                String itemID2 = v1[1];
                String num = tokens[1];

                Text k = new Text(itemID1);
                Text v = new Text("A:" + itemID2 + "," + num);

                context.write(k, v);

            } else if (flag.equals("step3")) {// flag为step3则输入的为评分矩阵，接下来标记为B
                String[] v2 = tokens[1].split(":");
                String itemID = tokens[0];
                String userID = v2[0];
                String pref = v2[1];

                Text k = new Text(itemID);
                Text v = new Text("B:" + userID + "," + pref);

                context.write(k, v);
            }
        }

    }

    /* 倒排后的评分矩阵：reduce过程输入的key是"itemID"，value是“B:userID1,评分”、“B:userID2,评分”、“B:userID3,评分”...;
    *  相似度矩阵：reduce过程输入的key是"itemID"，value是“A:itemID1,相似度”、“A:itemID2,相似度”、“A:itemID3,相似度”...;
    *  reduce输出的key是"userID"，value是”itemID,预测评分“*/
    public static class Step4_AggregateReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            System.out.println(key.toString() + ":");

            Map<String, String> mapA = new HashMap();//key:itemID,value:同现次数
            Map<String, String> mapB = new HashMap();//key:userID,value:评分

            for (Text line : values) {
                String val = line.toString();
//                System.out.println(val);

                if (val.startsWith("A:")) {
                    String[] kv = Recommend.DELIMITER.split(val.substring(2));
                    mapA.put(kv[0], kv[1]);

                } else if (val.startsWith("B:")) {
                    String[] kv = Recommend.DELIMITER.split(val.substring(2));
                    mapB.put(kv[0], kv[1]);

                }
            }

            /* 对于每个userID，计算它对此itemID（也就是key）的评分与此itemID和其他所有itemID（包含自己）的相似度的乘积，即从此itemID的视角下预测该用户对所有itemID的评分 */
            double result = 0;
            Iterator<String> iter = mapA.keySet().iterator();
            while (iter.hasNext()) {
                String mapk = iter.next();// 遍历与此itemID（也就是key）具有一定相似度的itemID（自己也包含了）

                int num = Integer.parseInt(mapA.get(mapk));
                Iterator<String> iterb = mapB.keySet().iterator();
                while (iterb.hasNext()) {
                    String mapkb = iterb.next();// userID
                    double pref = Double.parseDouble(mapB.get(mapkb));
                    result = num * pref;// 矩阵乘法相乘计算

                    Text k = new Text(mapkb);
                    Text v = new Text(mapk + "," + result);
                    context.write(k, v);
//                    System.out.println(k.toString() + "  " + v.toString());
                }
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Recommend.config();

        String input1 = path.get("Step4Input1");
        String input2 = path.get("Step4Input2");
        String output = path.get("Step4Output");

        HdfsOperator hdfs = new HdfsOperator(Recommend.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf);
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
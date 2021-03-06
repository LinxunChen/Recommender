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
import java.util.*;

/* EvaluateStep1负责计算每个用户产生的评分平方差的和以及计算数目 */
public class EvaluateStep1 {

    /* 测试集的输出：map过程输入的是"userID,itemID,pref“；输出的key是"userID"，value是"T:itemID,pref"
    * step5的输出：map过程输入的是"userID   itemID1:预测评分，itemID2:预测评分，itemID3:预测评分"；输出的key是"userID"，value是"R:itemID1,预测评分"*/
    public static class EvaluateEachMapper extends Mapper<Object, Text, Text, Text> {
        private String flag;// 标记，以区分不同的输入
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

            if (flag.equals("test")) {
                k.set(tokens[0]);
                v.set("T:" + tokens[1] + "," + tokens[2]);//添加T作为标记
                context.write(k, v);
            }

            else if (flag.equals("step5")) {
                k.set(tokens[0]);

                for (int i=1; i<tokens.length; i++) {
                    v.set("R:" + tokens[i].split(":")[0] + "," + tokens[i].split(":")[1]);//添加R作为标记
                    context.write(k, v);
                }
            }
        }
    }

    /* 测试集的输出：reduce过程输入的key是"userID"，value是"T:itemID1,pref"、"T:itemID2,pref"...
     step5的输出：reduce过程输入的key是"userID"，value是"R:itemID1,预测评分"、"R:itemID2,预测评分"...
    * reduce输出的key是"R"，value是”sum, count"*/
    public static class EvaluateEachReducer extends Reducer<Text, Text, Text, Text> {
        private Text k = new Text("R");
        private Text v = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> mapR = new HashMap();
            Map<String, Double> mapT = new HashMap();
            double sum = 0;
            int count = 0;

            for (Text line : values) {
                if (line.toString().startsWith("T")) {
                    mapT.put(line.toString().substring(2).split(",")[0], Double.parseDouble(line.toString().substring(2).split(",")[1]));
                }
                else if (line.toString().startsWith("R")) {
                    mapR.put(line.toString().substring(2).split(",")[0], Double.parseDouble(line.toString().substring(2).split(",")[1]));
                }
            }

            for (String item: mapR.keySet()) {
                if (mapT.containsKey(item)) {
                    double temp = Math.pow((mapR.get(item) - mapT.get(item)), 2);
                    sum += temp;
                    count++;
                }
            }

            v.set(new Double(sum).toString() + "," +  new Integer(count).toString());
            context.write(k, v);
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = Recommend.config();

        String input1 = path.get("EvaluateStep1Input1");
        String input2 = path.get("EvaluateStep1Input2");
        String output = path.get("EvaluateStep1Output");

        HdfsOperator hdfs = new HdfsOperator(Recommend.HDFS, conf);
        hdfs.rmr(output);
        hdfs.rmr(input1);
        hdfs.mkdirs(input1);
        hdfs.copyFile(path.get("Testing set"), input1);

        Job job = new Job(conf, "EvaluateStep1");
        job.setJarByClass(EvaluateStep1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(EvaluateStep1.EvaluateEachMapper.class);
        job.setReducerClass(EvaluateStep1.EvaluateEachReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}

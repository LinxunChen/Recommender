package org.lynnc.myhadoop.recommend;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

/* Step5负责对step4的结果进行合并求和，预测每个用户对所有itemID的评分，并进行推荐 */
public class Step5 {

    /* map过程输入的是"userID itemID,预测评分“；输出的key是"userID"，value是"itemID,预测评分" */
    public static class Step5_RecommendMapper extends Mapper<Object, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            k.set(tokens[0]);
            v.set(tokens[1]+","+tokens[2]);
            context.write(k, v);
        }
    }

    /* reduce过程输入的key是"userID"，value是"itemID1,预测评分"、"itemID1,预测评分""itemID2,预测评分""itemID3,预测评分"...（示例）
    * 输出的key是"userID"，value是["itemID1:总预测评分，itemID2:总预测评分，itemID3:总预测评分"...]（示例）*/
    public static class Step5_RecommendReducer extends Reducer<Text, Text, Text, Text> {
        private Text v = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> map = new HashMap();

            /* 将同一userID对同一itemID的预测评分进行求和，并将itemID和评分存在map中 */
            for (Text line : values) {
                String[] tokens = Recommend.DELIMITER.split(line.toString());
                String itemID = tokens[0];
                Double score = Double.parseDouble(tokens[1]);

                if (map.containsKey(itemID)) {
                    map.put(itemID, map.get(itemID) + score);// 矩阵乘法求和计算
                }
                else {
                    map.put(itemID, score);
                }
            }

            /* 对map进行按value的降序排序，得到newMap */
            ArrayList<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
                @Override
                public int compare(Map.Entry<String, Double> arg0, Map.Entry<String, Double> arg1) {
                return (int)(arg1.getValue() - arg0.getValue());
                }
            });

            Map<String, Double> newMap = new LinkedHashMap();
            for (int i = 0; i < list.size(); i++) {
                newMap.put(list.get(i).getKey(), list.get(i).getValue());
            }

            /* 过滤newMap中该用户（即key）已经评价过的物品，得到最终的推荐列表 */
            List<String> recList = new ArrayList();
            for (String itemKey : newMap.keySet()) {
                if (Step1.userItem.get(key.toString()).indexOf(itemKey) == -1) {
                    recList.add(itemKey + ':' + newMap.get(itemKey));
                }
            }

            v.set(recList.toString());

            context.write(key, v);
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Recommend.config();

        String input = path.get("Step5Input");
        String output = path.get("Step5Output");

        HdfsOperator hdfs = new HdfsOperator(Recommend.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf);
        job.setJarByClass(Step5.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step5.Step5_RecommendMapper.class);
        job.setReducerClass(Step5.Step5_RecommendReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

}
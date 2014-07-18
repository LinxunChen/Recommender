package org.lynnc.myhadoop.recommend;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
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

/* Step5负责对step4的结果进行合并求和，预测每个用户对所有itemID的评分，并进行推荐 */
public class Step5 {

    /* step4的输出：map过程输入的是"userID itemID,预测评分,相似度“；输出的key是"userID"，value是"itemID,预测评分,相似度"
    * step2_2的输出：map过程输入的是"userID   itemID1:评分，itemID2:评分，itemID3:评分"；输出的key是"userID"，value是"CitemID1:评分，itemID2:评分，itemID3:评分"*/
    public static class Step5_RecommendMapper extends Mapper<Object, Text, Text, Text> {
        private String flag;// 标记，以区分
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

            if (flag.equals("step4")) {
                k.set(tokens[0]);
                v.set(tokens[1] + "," + tokens[2] + "," + tokens[3]);
                context.write(k, v);
            }

            else if (flag.equals("step2_2")) {
                k.set(tokens[0]);

                StringBuilder sb = new StringBuilder();
                for (int i=1; i<tokens.length; i++) {
                    sb.append("," + tokens[i]);
                }

                v.set(sb.toString().replaceFirst(",", "C"));//添加C作为标记
                context.write(k, v);
            }
        }
    }

    /* step4的输出：reduce过程输入的key是"userID"，value是"itemID1,预测评分,相似度"、"itemID1,预测评分,相似度""itemID2,预测评分,相似度""itemID3,预测评分,相似度"...（示例）（给用户计算推荐的物品）
     step2_2的输出：reduce过程输入的key是"userID"，value是"CitemID1:评分，itemID2:评分，itemID3:评分"（用户已评分的物品）
    * 输出的key是"userID"，value是"itemID1:总预测评分，itemID2:总预测评分，itemID3:总预测评分"...（示例）*/
    public static class Step5_RecommendReducer extends Reducer<Text, Text, Text, Text> {
        private Text v = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> map = new HashMap();
            Map<String, Double> mapResult = new HashMap();
            Map<String, Double> mapSim = new HashMap();
            List<String> prefItems = new ArrayList<String>();


            for (Text line : values) {
                /* 得到用户key的行为记录列表list */
                if (line.toString().startsWith("C")) {
                    String[] tokens = Recommend.DELIMITER.split(line.toString().substring(1));
                    for (int i=0; i<tokens.length; i++) {
                        prefItems.add(tokens[i].split(":")[0]);
                    }
                }

                /* 将同一userID对同一itemID的预测评分进行求和，并将itemID、评分和相似度存在map中 */
                else {
                    String[] tokens = Recommend.DELIMITER.split(line.toString());
                    String itemID = tokens[0];
                    Double score = Double.parseDouble(tokens[1]);
                    Double sim = Double.parseDouble(tokens[2]);

                    if (sim >= Recommend.simThreshold) {
                        if (mapResult.containsKey(itemID)) {
                            mapResult.put(itemID, mapResult.get(itemID) + score);// 矩阵乘法求和计算
                        }
                        else {
                            mapResult.put(itemID, score);
                        }

                        if (mapSim.containsKey(itemID)) {
                            mapSim.put(itemID, mapSim.get(itemID) + sim);// 矩阵乘法求和计算
                        }
                        else {
                            mapSim.put(itemID, sim);
                        }
                    }
                }
            }

            /* 计算最终的推荐评分，并过滤map中该用户（即key）已经评价过的物品 */
            for (String itemID : mapResult.keySet()) {
                if (prefItems.indexOf(itemID) == -1) {
                    map.put(itemID, Math.round((mapResult.get(itemID)/mapSim.get(itemID))*10000)/10000.0 );
                }
            }

            /* 对map进行按value的降序排序，得到newMap */
            ArrayList<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
                @Override
                public int compare(Map.Entry<String, Double> arg0, Map.Entry<String, Double> arg1) {
                return (int)((arg1.getValue() - arg0.getValue())*10000);
                }
            });

            Map<String, Double> newMap = new LinkedHashMap();
            for (int i = 0; i < list.size(); i++) {
                newMap.put(list.get(i).getKey(), list.get(i).getValue());
            }

            StringBuilder sb = new StringBuilder();
            for (String itemKey : newMap.keySet()) {
                sb.append("," + itemKey + ':' + newMap.get(itemKey));
            }

            v.set(sb.toString().replaceFirst(",", ""));
            context.write(key, v);
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Recommend.config();

        String input1 = path.get("Step5Input1");
        String input2 = path.get("Step5Input2");
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

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
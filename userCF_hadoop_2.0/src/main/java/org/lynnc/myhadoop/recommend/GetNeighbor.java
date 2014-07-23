package org.lynnc.myhadoop.recommend;

import org.apache.hadoop.fs.Path;
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

import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.util.*;

/* GetNeighbor负责生成每个用户的邻域列表 */
public class GetNeighbor {

    /* map过程输入的是"userID1:userID2 相似度“; 输出的key是"userID1"，value是“userID2:相似度” */
    public static class getNeighborMapper extends Mapper<Object, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());

            k.set(tokens[0].split(":")[0]);
            v.set(tokens[0].split(":")[1] + ":" + tokens[1]);
            context.write(k, v);
        }
    }

    /* reduce过程输入的key是"userID1",value是"userID2:相似度“,"userID3:相似度“,"userID4:相似度“...; 输出的key是"userID1"，value是“userID2:相似度, userID4:相似度,userID3:相似度”(已排序) */
    public static class getNeighborReducer extends Reducer<Text, Text, Text, Text> {
        private Text v = new Text();
        private int topKNeighbors = 10;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> mapSim = new HashMap<String, Double>();
            Map<String, Double> orderedMapSim = new LinkedHashMap();

            for (Text line: values) {
                if (!line.toString().split(":")[0].equals(key.toString())) {
                    mapSim.put(line.toString().split(":")[0],Math.round(Double.parseDouble(line.toString().split(":")[1])*10000)/10000.0);
                }
            }

            /* 对mapSim进行按value的降序排序，得到orderedMap */
            ArrayList<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(mapSim.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
                @Override
                public int compare(Map.Entry<String, Double> arg0, Map.Entry<String, Double> arg1) {
                    return (int) ((arg1.getValue() - arg0.getValue()) * 10000);
                }
            });

            for (int i = 0; i < list.size(); i++) {
                orderedMapSim.put(list.get(i).getKey(), list.get(i).getValue());
            }

            StringBuilder sb = new StringBuilder();
            int count = 0;
            for (String userID: orderedMapSim.keySet()) {
                sb.append("," + userID + ":" + orderedMapSim.get(userID));
                count++;
                if (count >= topKNeighbors) {
                    break;
                }
            }

            v.set(sb.toString().replaceFirst(",", ""));
            context.write(key, v);
        }
    }

    public static void run(Map<String, String> path) throws FileSystemNotFoundException, IOException, InterruptedException, ClassNotFoundException {

        JobConf conf = Recommend.config();

        String input = path.get("GetNeighborInput");
        String output = path.get("GetNeighborOutput");

        HdfsOperator hdfs = new HdfsOperator(Recommend.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf);
        job.setJarByClass(GetNeighbor.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(GetNeighbor.getNeighborMapper.class);
        job.setReducerClass(GetNeighbor.getNeighborReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}

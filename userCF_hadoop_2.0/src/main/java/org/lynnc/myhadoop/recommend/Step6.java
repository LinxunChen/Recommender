package org.lynnc.myhadoop.recommend;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.lynnc.myhadoop.hdfs.HdfsOperator;

import java.io.IOException;
import java.util.Map;

/* Step6负责对step5推荐结果进行过滤，推荐前RecNum个物品 */
public class Step6 {

    /* map过程输入的是“userID     itemID1:总预测评分，itemID2:总预测评分，itemID3:总预测评分"; 输出的key是"userID"，value是"itemID1:总预测评分，itemID2:总预测评分，itemID3:总预测评分" */
    public static class Step6_RecommendFilterMapper extends Mapper<Object, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            StringBuilder sb = new StringBuilder();

            String userID = tokens[0];
            for (int i=1; i<=Recommend.RecNum && i<tokens.length; i++ ) {
                sb.append("," + tokens[i]);
            }

            if (userID.equals(Recommend.Person)) {
                System.out.println("给Person用户的推荐为：  ");
                System.out.println(userID + ":" + sb.toString().replaceFirst(",", ""));
            }

            k.set(userID);
            v.set(sb.toString().replaceFirst(",", ""));
            context.write(k, v);
        }
    }

    public static void run (Map<String, String> path) throws IOException,InterruptedException, ClassNotFoundException {
        Configuration conf = Recommend.config();

        String input = path.get("Step6Input");
        String output = path.get("Step6Output");

        HdfsOperator hdfs = new HdfsOperator(Recommend.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf, "Step6");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step6_RecommendFilterMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}

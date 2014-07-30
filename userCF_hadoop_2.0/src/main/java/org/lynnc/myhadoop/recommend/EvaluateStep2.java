package org.lynnc.myhadoop.recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.Map;

/* EvaluateStep2合并所有的评分平方差的和以及计算数目，计算RMSE */
public class EvaluateStep2 {

    /* map过程输入的是"R   sum,count"；输出的key是"R"，value是"sum,count"*/
    public static class EvaluateAggregateMapper extends Mapper<Object, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());

            k.set(tokens[0]);
            v.set(tokens[1] + "," + tokens[2]);
            context.write(k, v);
        }
    }

    /* reduce过程输入的key是"R"，value是"sum,count","sum,count","sum,count","sum,count"...
    * reduce输出的key是"RMSE"，value是”RMSE结果*/
    public static class EvaluateAggregateReducer extends Reducer<Text, Text, Text, Text> {
        private Text k = new Text("RMSE");
        private Text v = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (Text line : values) {
                sum += Double.parseDouble(line.toString().split(",")[0]);
                count += Integer.parseInt(line.toString().split(",")[1]);
            }

            Double RMSE = new Double(Math.sqrt(sum/count));

            v.set(RMSE.toString());
            context.write(k, v);
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = Recommend.config();

        String input = path.get("EvaluateStep2Input");
        String output = path.get("EvaluateStep2Output");

        HdfsOperator hdfs = new HdfsOperator(Recommend.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf, "EvaluateStep2");
        job.setJarByClass(EvaluateStep2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(EvaluateStep2.EvaluateAggregateMapper.class);
        job.setReducerClass(EvaluateStep2.EvaluateAggregateReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}

package org.lynnc.myhadoop.recommend;

import org.apache.hadoop.mapred.JobConf;
import org.lynnc.myhadoop.hdfs.HdfsOperator;

import java.io.IOException;

public class Demo {
    public static void main (String[] args) throws IOException {
        JobConf conf = Recommend.config();
        HdfsOperator hdfs = new HdfsOperator(conf);
        hdfs.ls("/user/lynnc/reco./mmend/step5");
        hdfs.cat("/user/lynnc/recommend/step5/part-r-00000");
    }
}



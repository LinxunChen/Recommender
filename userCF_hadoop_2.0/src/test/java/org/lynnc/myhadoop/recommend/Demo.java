package org.lynnc.myhadoop.recommend;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.mapred.JobConf;
import org.lynnc.myhadoop.hdfs.HdfsOperator;

import java.io.IOException;

public class Demo {
    public static void main (String[] args) throws IOException {
        JobConf conf = Recommend.config();
        HdfsOperator hdfs = new HdfsOperator(conf);
//        hdfs.ls("/user/lynnc/recommend/step2_1");//显示文件夹下有哪些文件
//        hdfs.cat("/user/lynnc/recommend/step6/part-r-00000");//对每个用户推荐RecNum个物品
        hdfs.cat("/user/lynnc/recommend/evaluation/part-r-00000");//显示RMSE（count最大时对应的RMSE为所需）
//        hdfs.cat("/user/lynnc/recommend/neighbors/part-r-00000");//获取每个用户的邻域推荐
    }
}
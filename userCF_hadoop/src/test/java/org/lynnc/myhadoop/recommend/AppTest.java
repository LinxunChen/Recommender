package org.lynnc.myhadoop.recommend;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.mapred.JobConf;
import org.lynnc.myhadoop.hdfs.HdfsOperator;

import java.io.IOException;

/**
 * Unit test for simple App.
 */
public class AppTest {
    public static void main (String[] args) throws IOException {
        JobConf conf = Recommend.config();
        HdfsOperator hdfs = new HdfsOperator(conf);
        hdfs.ls("/user/lynnc/recommend/step2");
        hdfs.cat("/user/lynnc/recommend/step2/part-r-00000");
//        String a = "2\t101:2.0,102:2.5,103:5.0,104:2.0";
//        String[] tokens = Recommend.DELIMITER.split(a);
//        System.out.println(tokens[0]);
    }
}
package org.lynnc.myhadoop.recommend;


import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class Recommend {

    public static final String HDFS = "hdfs://localhost:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static double simThreshold = 0.0; //判断用户邻域采用的用户相似度阈值 [0,1]
    public static int RecNum = 20;  //推荐物品的数量
    public static String Person = "7";  //显示该用户的推荐列表

    public static void main( String[] args ) throws InterruptedException, ClassNotFoundException, IOException{
        Map<String, String> path = new HashMap();

        path.put("Trainning set", "/home/lynnc/ua.base");
        path.put("Testing set", "/home/lynnc/ua.test");
//        path.put("Trainning set", "/home/lynnc/small2.csv");
//        path.put("Testing set", "/home/lynnc/small2test.csv");

        path.put("Step1Input", HDFS + "/user/lynnc/recommend");
        path.put("Step1Output", path.get("Step1Input") + "/step1");

        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output1", path.get("Step1Input") + "/step2_1");
        path.put("Step2Output2", path.get("Step1Input") + "/step2_2");

        path.put("Step3Input", path.get("Step1Output"));
        path.put("Step3Output", path.get("Step1Input") + "/step3");

        path.put("Step4Input1", path.get("Step2Output1"));
        path.put("Step4Input2", path.get("Step3Output"));
        path.put("Step4Output", path.get("Step1Input") + "/step4");

        path.put("Step5Input1", path.get("Step4Output"));
        path.put("Step5Input2", path.get("Step2Output2"));
        path.put("Step5Output", path.get("Step1Input") + "/step5");

        path.put("Step6Input", path.get("Step5Output"));
        path.put("Step6Output", path.get("Step1Input") + "/step6");

        path.put("EvaluateInput1", HDFS + "/user/lynnc/recommend/test");
        path.put("EvaluateInput2", path.get("Step5Output"));
        path.put("EvaluateOutput", path.get("Step1Input") + "/evaluation");

        path.put("GetNeighborInput", path.get("Step3Output"));
        path.put("GetNeighborOutput", path.get("Step1Input") + "/neighbors");

        Step1.run(path);
        Step2.run(path);
        Step3.run(path);
        Step4.run(path);
        Step5.run(path);
        Step6.run(path);
        Evaluate.run(path);
        GetNeighbor.run(path);

        System.exit(0);
    }

    public static JobConf config() {
        JobConf conf = new JobConf(Recommend.class);
        conf.setJobName("Recommend");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
}

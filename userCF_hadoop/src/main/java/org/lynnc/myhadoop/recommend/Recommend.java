package org.lynnc.myhadoop.recommend;


import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class Recommend {

    public static final String HDFS = "hdfs://localhost:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main( String[] args ) throws InterruptedException, ClassNotFoundException, IOException{
        Map<String, String> path = new HashMap();

        path.put("data", "/home/lynnc/small.csv");
        path.put("Step1Input", HDFS + "/user/lynnc/recommend");
        path.put("Step1Output", path.get("Step1Input") + "/step1");

        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("Step1Input") + "/step2");

        path.put("Step3Input", path.get("Step1Output"));
        path.put("Step3Output", path.get("Step1Input") + "/step3");

        path.put("Step4Input1", path.get("Step3Output"));
        path.put("Step4Input2", path.get("Step2Output"));
        path.put("Step4Output", path.get("Step1Input") + "/step4");

        path.put("Step5Input", path.get("Step4Output"));
        path.put("Step5Output", path.get("Step1Input") + "/step5");

        Step1.run(path);
        Step2.run(path);
//        Step3.run(path);
//        Step4.run(path);
//        Step5.run(path);

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

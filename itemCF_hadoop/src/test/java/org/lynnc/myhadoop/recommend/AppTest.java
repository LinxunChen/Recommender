package org.lynnc.myhadoop.recommend;

import org.apache.hadoop.mapred.JobConf;
import org.lynnc.myhadoop.hdfs.HdfsOperator;

import java.io.IOException;

/**
 * Unit test for simple App.
 */
//public class AppTest
//    extends TestCase
//{
//    /**
//     * Create the test case
//     *
//     * @param testName name of the test case
//     */
//    public AppTest( String testName )
//    {
//        super( testName );
//    }
//
//    /**
//     * @return the suite of tests being tested
//     */
//    public static Test suite()
//    {
//        return new TestSuite( AppTest.class );
//    }
//
//    /**
//     * Rigourous Test :-)
//     */
//    public void testApp()
//    {
//        assertTrue( true );
//    }
//}
public class AppTest {
    public static void main (String[] args) throws IOException {
        JobConf conf = Recommend.config();
        HdfsOperator hdfs = new HdfsOperator(conf);
        hdfs.ls("/user/lynnc/recommend/step5");
        hdfs.cat("/user/lynnc/recommend/step5/part-r-00000");
//        String a = "2\t101:2.0,102:2.5,103:5.0,104:2.0";
//        String[] tokens = Recommend.DELIMITER.split(a);
//        System.out.println(tokens[0]);
    }
}



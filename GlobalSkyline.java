package org.myorg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.URI;
import java.math.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.DistributedCache;

public class GlobalSkyline extends Configured implements Tool {
  public static final int DIM = 9;
  public static final int TWO_TO_THE_POWER_DIM = (1<<9);  // 512
  public static final String DIM_SIZE_STRING = "123456789";
  
  public static final int[] pk_positions = {0,6,  14,22};  // [begin1, end1), [begin2, end2) : STN, YYYYMMDD
  public static final int[] value_positions = {24,30,  35,41,  46,52,  102,108,     57,63,  78,83,  88,93,  95,100,  110,116};
  public static final int[] value_type = {-1, -1, -1, -1,  1, 1, 1, 1, 1};  // -1 = maximize, +1 = minimize. 

  public static final int PLUS_TYPE = 0;
  public static final int STAR_TYPE = 1;

  public static final int DOMINATES = -1;
  public static final int DOMINATED = -1;
  

  public static double[] minvalues;
  public static double[] maxvalues;

  // Returns
  //  < 0 if sub1 DOMINATES sub2
  //  = 0 if same [i.e. substring[0, minimums length] matches.
  //  > 0 if sub2 DOMINATES sub1
  // Examples
  //  "" == 010011 (anything)
  //  0 == 0
  //  0 < 1
  //  0 == 01
  //  11 == 1100
  //  11 > 101
  //  11 > 101
  //  10 > 01
  public static int compare(String sub1, String sub2) {
    int ml = Math.min(sub1.length(), sub2.length());
    return sub1.substring(0, ml).compareTo(sub2.substring(0, ml));
  }
  
  private static String sub(String id, int k) {  // 0 <= k < DIM
    //if (k < 0 || k >= DIM) throw new RuntimeException("k out of correct range");
    StringBuilder sb = new StringBuilder();
    for (int i = k; i < id.length(); i += DIM) {
      sb.append(id.charAt(i));
    }
    return sb.toString();
  }

  public static boolean isNeeded(String id1, String id2) {
    for (int k = 0; k < DIM; ++k) {
      if (compare(sub(id1, k), sub(id2, k)) != 0) return false;
    }
    return true;
  }
  
  public int run(String[] args) throws Exception {
    Configuration config = getConf();
    FileSystem dfs = FileSystem.get(config);
    
    JobConf conf = new JobConf(getConf(), GlobalSkyline.class);
 
    Path hdfsPath = new Path("/user/cloudera/data/allvpn");
    dfs.copyFromLocalFile(false, true, new Path("vpn"), hdfsPath);
    DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
    
    hdfsPath = new Path("/user/cloudera/data/allfilter");
    dfs.copyFromLocalFile(false, true, new Path("filter"), hdfsPath);
    DistributedCache.addCacheFile(hdfsPath.toUri(), conf);

    conf.setJobName("GlobalSkyline");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(GSkyMapper.class);
    conf.setCombinerClass(GSkyReducer.class);
    conf.setReducerClass(GSkyReducer.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setJarByClass(GlobalSkyline.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
    return 0;
  }

	public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GlobalSkyline(), args);
    System.exit(res);
  }
}

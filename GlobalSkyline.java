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

//  public static QTNode root;
  
  public static class Point {
    public int[] pk;
    public double[] value;
    
    public Point() {
      pk = new int[pk_positions.length / 2];
      value = new double[value_positions.length / 2];
      for (int i = 0; i < pk.length; ++i) {
        pk[i] = -1;
      }
      for (int i = 0; i < value.length; ++i) {
        value[i] = -1e20;  // -inf
      }
    }

    public boolean parseFromRawLine(String line) {  // When scanning raw input file in mapper of L-SKY-MR.
      if (line.equals("")) return false;
      
      // Read PK
      for (int i = 0; i < pk_positions.length; i += 2) {
        pk[i/2] = Integer.parseInt(line.substring(pk_positions[i], pk_positions[i+1]));
      }

      // Read value
      for (int i = 0; i < value_positions.length; i += 2) {
        // value_type will reverse the values.
        value[i/2] = value_type[i/2] * Double.parseDouble(line.substring(value_positions[i], value_positions[i+1]));
      }
      return true;
    }
    
    public void parseFromString(String line) {  // E.g. scanning String formed by p.ToString(), scanned inside reducer of L-SKY-MR.
  		String delims = ",";
  		String[] tokens = line.split(delims);
      for (int i = 0; i < pk.length; ++i) {
        pk[i] = Integer.parseInt(tokens[i]);
      }
      for (int i = 0; i < value.length; ++i) {
        value[i] = Double.parseDouble(tokens[pk.length + i]);
      }
    }

    public String formatPK() {
      return String.format("%d_%d_%d", pk[0], pk[1]/10000, pk[1]%10000);
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < pk.length; ++i) {
        if (i > 0) sb.append(",");
        sb.append(Integer.toString(pk[i]));
      }
      for (int i = 0; i < value.length; ++i) {
        sb.append(",");
        sb.append(Double.toString(value[i]));
      }
      return sb.toString();
    }

    // returns true if 'this' dominates (MIN) 'p'.
    public boolean dominates(Point p) {
      boolean less_found = false;
      for (int i = 0; i < value.length; ++i) {
        if (value[i] > p.value[i]) return false;
        if (value[i] < p.value[i]) less_found = true;
      }
      return less_found;
    }

    // returns
    // -1  this dominates p
    //  0  incomparable (no point dominates the other)
    //  1  p dominates this  
    public int compare(Point p) {
      int this_is_less_than_p = 0;
      int p_is_less_than_this = 0;
      for (int i = 0; i < value.length; ++i) {
        if (value[i] > p.value[i]) p_is_less_than_this = 1;
        else if (value[i] < p.value[i]) this_is_less_than_p = -1;
      }
      return p_is_less_than_this + this_is_less_than_p;
    }
  }

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
//      if (k < 0 || k >= DIM) throw new RuntimeException("k out of correct range");
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




  public static class GSkyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public TreeMap<String, Point> vpns;
    public Vector<String> ids;
    public Vector<Point> filters;
  
    public void configure(JobConf job) {
      String vpnfile = new Path("/user/cloudera/data/allvpn").getName();
      String filterfile = new Path("/user/cloudera/data/allfilter").getName();

      ids = new Vector<String>();
      filters = new Vector<Point>();
      vpns = new TreeMap<String, Point>();

      try {
        Path [] cacheFiles = DistributedCache.getLocalCacheFiles(job);
        String filename = null;
        
        if (null != cacheFiles && cacheFiles.length > 0) {
          for (Path cachePath : cacheFiles) {
            if (cachePath.getName().equals(vpnfile)) {
              filename = cachePath.toString();
              break;
            }
          }
        }

        BufferedReader reader =
            new BufferedReader(new FileReader(new File(filename)));
        String line;
        while ((line = reader.readLine()) != null) {
          Scanner sc = new Scanner(line);
          String id = sc.next();
          ids.add(id);
          
          Point p = new Point();
          p.parseFromString(sc.next());
          vpns.put(id, p);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      
      
      try {
        Path [] cacheFiles = DistributedCache.getLocalCacheFiles(job);
        String filename = null;
        
        if (null != cacheFiles && cacheFiles.length > 0) {
          for (Path cachePath : cacheFiles) {
            if (cachePath.getName().equals(filterfile)) {
              filename = cachePath.toString();
              break;
            }
          }
        }

        BufferedReader reader =
            new BufferedReader(new FileReader(new File(filename)));
        String line;
        while ((line = reader.readLine()) != null) {
          Scanner sc = new Scanner(line);
          String id = sc.next(); //  unused
          
          Point p = new Point();
          p.parseFromString(sc.next());
          filters.add(p);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      Scanner sc = new Scanner(value.toString());
      String id = sc.next();
      Point p = new Point();
      p.parseFromString(sc.next());

      // If dominated by Sky-filter points then return...
      for (Point f : filters) {
        if (f.dominates(p)) {
          return;
        }
      }
      
      Text k = new Text(id);
      output.collect(k, new Text("+" + p.toString()));

      for (String other_id : ids) {
        if (other_id.equals(id) == false && isNeeded(id, other_id)) {
          if (p.dominates(vpns.get(other_id))) {  // If 'p' dominates the 'VPn' of 'node'.
            output.collect(k, new Text("*" + p.toString()));
          }
        }
      }
    }
  }  

  // Done
  public static class GSkyReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    private static final Text empty_key = new Text(new String(""));
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      Vector<Point> plus_points = new Vector<Point>();  // + points
      Vector<Point> star_points = new Vector<Point>();  // * points

		  while (values.hasNext()) {
		    String sp = values.next().toString();
	      Point p = new Point();
	      p.parseFromString(sp.substring(1));
		    if (sp.charAt(0) == '+') {
			    plus_points.add(p);
			  } else {
			    star_points.add(p);
			  }
			}

			// check
			for (Point p : plus_points) {
			  boolean dominated = false;
			  for (Point pd : star_points) {
			    if (pd.dominates(p)) {
			      dominated = true;
			      break;
			    }
			  }
			  if (dominated) continue;

			  output.collect(key, new Text("+" + p.toString()));
//			  output.collect(empty_key, new Text(p.formatPK()));
			} 

		  for (Point pd : star_points) {
			  output.collect(key, new Text("*" + pd.toString()));      }
    }
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

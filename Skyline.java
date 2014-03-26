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
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.DistributedCache;

public class Skyline extends Configured implements Tool {
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

  // O(n^2*dim)
  public static Vector<Point> GSKY(Vector<Point> p) {
    Vector<Point> sky = new Vector<Point>();
    Vector<Boolean> is_dominated = new Vector<Boolean>();

    for (int i = 0; i < p.size(); ++i) {
      int comp = 0;
      for (int j = 0; j < sky.size(); ++j) {
        comp = p.elementAt(i).compare(sky.elementAt(j));
        if (comp == 1) break;
        if (comp == -1) {
          is_dominated.set(j, Boolean.TRUE);
        }
      }
      if (comp == 1) continue;

      // add new skyline point and 'false' that it is not dominated (at least yet).
      sky.add(p.elementAt(i));
      is_dominated.add(Boolean.FALSE);
    }

    // Remove all dominated
    Vector<Point> skyfinal = new Vector<Point>();
    for (int i = 0; i < sky.size(); ++i) {
      if (is_dominated.elementAt(i) == Boolean.FALSE) skyfinal.add(sky.elementAt(i));
    }
    return skyfinal;
  }

  public static class QTNode {  
    public int depth;
    public String id;  // length = DIM * depth
    public QTNode[] children;  // either null (leaf node) or 2^DIM size (internal node).
    public Vector<Point> points;  // either null (internal node) or 0 or more points (leaf node).

    public double[] lo;  // length = DIM
    public double[] hi;  // length = DIM

    // DIM=5, num=13 >>> 01101
    public String numToOneDepthId(int num) {
      StringBuilder sb = new StringBuilder(DIM_SIZE_STRING);
      for (int i = DIM - 1; i >= 0; --i) {
        sb.setCharAt(i, (char)('0' + num%2));
        num /= 2;
      }
      return sb.toString();
    }

    public QTNode(int depth, String id, Vector<Point> points, double[] lo, double[] hi) {
      this.depth = depth;
      this.id = id;
      this.children = null;
      this.points = points;
      this.lo = lo;
      this.hi = hi;

      if (id.length() != depth * DIM) throw new RuntimeException("|id| != depth*DIM");
      if (lo.length != DIM) throw new RuntimeException("|lo| != DIM");
      if (hi.length != DIM) throw new RuntimeException("|hi| != DIM");
    }

    // Returns -1 if this is a leaf node (i.e. no childs).
    // Returns the child index in which this point will go.
    // NOTE: this does not check whether the given point is outside the region of this QTNode.
    // It MUST be inside the QTNode.
    public int childIndex(Point p) {
      if (children == null) return -1;

      int chi = 0;
      for (int i = 0; i < DIM; ++i) {
        if (p.value[i] >= (lo[i] + hi[i]) / 2) {  // upper half, so add 1.
          chi |= (1<<i);
        }
      }
      return chi;
    }

    public void divide() {
      divide(20);
    }

    // maxp = threshold count for dividing QuadTrees.
    public void divide(int maxp) {
      if (points.size() <= maxp) return;

      children = new QTNode[TWO_TO_THE_POWER_DIM];
      ArrayList<Vector<Point>> chi_points = new ArrayList<Vector<Point>>(TWO_TO_THE_POWER_DIM);
      double[][] chi_lo = new double[TWO_TO_THE_POWER_DIM][DIM];
      double[][] chi_hi = new double[TWO_TO_THE_POWER_DIM][DIM];

      // compute lo and hi, and create new vectors.
      for (int chi = 0; chi < TWO_TO_THE_POWER_DIM; ++chi) {
        chi_points.add(new Vector<Point>());
        for (int i = 0; i < DIM; ++i) {
          if ((chi & (1<<i)) == 0) {  // lower half
            chi_lo[chi][i] = lo[i];
            chi_hi[chi][i] = (hi[i] + lo[i]) / 2;
          } else {  // upper half
            chi_lo[chi][i] = (hi[i] + lo[i]) / 2;
            chi_hi[chi][i] = hi[i];
          }
        }
      }

      // move point to appropriate child
      for (Point p : points) {
        chi_points.get(childIndex(p)).add(p);
      }

      // create child QTNode's.
      for (int chi = 0; chi < TWO_TO_THE_POWER_DIM; ++chi) {
        children[chi] = new QTNode(depth + 1, id + numToOneDepthId(chi), chi_points.get(chi), chi_lo[chi], chi_hi[chi]);
      }

      // remove points from this.
      points = null;

      // prune 11...1 if 00...0 has at least one point.
      if (children[0].points.size() > 0) {
        children[TWO_TO_THE_POWER_DIM - 1] = null;
      }

      // call this recursively on each child.
      for (int chi = 0; chi < TWO_TO_THE_POWER_DIM; ++chi) {
        if (children[chi] != null) {
          children[chi].divide(maxp);
        }
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
    public int compare(String sub1, String sub2) {
      int ml = Math.min(sub1.length(), sub2.length());
      return sub1.substring(0, ml).compareTo(sub2.substring(0, ml));
    }

    private String sub(int k) {  // 0 <= k < DIM
//      if (k < 0 || k >= DIM) throw new RuntimeException("k out of correct range");
      StringBuilder sb = new StringBuilder();
      for (int i = k; i < id.length(); i += DIM) {
        sb.append(id.charAt(i));
      }
      return sb.toString();
    }

    // Returns true if this dominates q.
    public boolean dominates(QTNode q) {
      boolean less_found = false;
      for (int k = 0; k < DIM; ++k) {
        int cmp = compare(sub(k), q.sub(k));
        if (cmp > 0) return false;
        if (cmp < 0) less_found = true;
      }
      return less_found;
    }

    // Returns true if this may or may not dominate q. E.g.
    // 01 vs 10 --> false (no domination possible for sure)
    // 00 vs 11 --> false (surely dominate)
    // 00 vs 01 --> true
    // 00 vs 10 --> true
    public boolean mayOrMayNotDominate(QTNode q) {
      for (int k = 0; k < DIM; ++k) {
        if (compare(sub(k), q.sub(k)) != 0) return false;
      }
      return true;
    }

    // Returns
    // -1  this dominates q
    //  0  incomparable
    //  1  q dominates this  
    public int compare(QTNode q) {
      int this_is_less_than_q = 0;
      int q_is_less_than_this = 0;
      for (int k = 0; k < DIM; ++k) {
        int cmp = compare(sub(k), q.sub(k));
        if (cmp > 0) q_is_less_than_this = 1;
        else if (cmp < 0) this_is_less_than_q = -1;
      }
      return q_is_less_than_this + this_is_less_than_q;
    }
  }

  // Returns the id of the leaf-QuadTreeNode where p belongs.
  // If the node is pruned then it returns null.
  public static String getNodeId(QTNode root, Point p) {
    QTNode q = root;
    while (true) {
      int chi = q.childIndex(p);
      if (chi == -1) break;
      q = q.children[chi];
      if (q == null) return null;
    }
    return q.id;
  }


/*

*           children != null, children[3] == null , points = null
 00         children != null, children[3] == null , points = null
   0000(p1) children = null                       , points = {p1}
   0001     children = null                       , points = {}
   0010     children = null                       , points = {}
   0011(p2) X
 01         children = null                       , points = {}
 10         children = null                       , points = {}
 11         X

*/

  public static class LSkyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public QTNode root;
    int map_called;

    public void configure(JobConf job) {
      map_called = 0;

      //System.out.println("in configure!");
      String qtfilename = new Path("/user/cloudera/data/qtd.txt").getName();
//      System.out.println("!!! " + qtfilename);      

      try {
        Path [] cacheFiles = DistributedCache.getLocalCacheFiles(job);
        String filename = null;
        
        if (null != cacheFiles && cacheFiles.length > 0) {
          for (Path cachePath : cacheFiles) {
//            System.out.println(">>> " + cachePath.getName() + " | " + cachePath.toString());
            if (cachePath.getName().equals(qtfilename)) {
//              System.out.println(">>>paisi " + cachePath.getName() + " | " + cachePath.toString());
              filename = cachePath.toString();
              break;
            }
          }
        }

        root = readQT(filename);
        //if (root == null) System.out.println("ay hay!!!!");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
      
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      map_called++;
      //if (map_called % 100 == 0) System.out.println("map called >>>> " + map_called);

      String line = value.toString();
      if (line.trim().equals("")) return;  // blank line

      Point p = new Point();
      p.parseFromRawLine(line);
      
      //if (root == null) System.out.println("ay hay!!!! mapper");
      String id = getNodeId(root, p);  // QTNodes...

      if (id == null) {
        System.out.println("LSkyMapper::map: point in pruned node. returning.");
        return;  // Inside some pruned node of quadtree
      }

      output.collect(new Text(id), new Text(p.toString()));
    }
  }  

  public static class LSkyReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public MultipleOutputs mos;

    public void configure(JobConf job) {
      mos = new MultipleOutputs(job); 
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      // compute local skyline
      Vector<Point> points = new Vector<Point>();
		  while (values.hasNext()) {
		    Point p = new Point();
		    p.parseFromString(values.next().toString());
			  points.add(p);
			}
			Vector<Point> skyline = GSKY(points);

			// Output all points in skyline
			Point vpn = new Point();
			int[] filter = new int[DIM];
		  for (int i = 0; i < DIM; ++i) {
		    filter[i] = -1;
		  }
		  
		  int j = 0;
			for (Point p : skyline) {
			  output.collect(key, new Text(p.toString()));
			  for (int i = 0; i < DIM; ++i) {
			    if (p.value[i] > vpn.value[i]) {
			      vpn.value[i] = p.value[i];
			    }
			    if (filter[i] == -1 || p.value[i] < skyline.elementAt(filter[i]).value[i]) {
			      filter[i] = j;
			    }
			  }
			  ++j;
			}
			Arrays.sort(filter);

			// Output VPn
			mos.getCollector("vpn", reporter).collect(key, vpn.toString());
			
			// Output all skyfilter points
			mos.getCollector("filter", reporter).collect(key, skyline.elementAt(filter[0]).toString());
			for (int i = 1; i < DIM; ++i) {
			  if (filter[i] != filter[i-1]) {
    			mos.getCollector("filter", reporter).collect(key, skyline.elementAt(filter[i]).toString());
			  }
			}
    }
  }



  public static int INTERNAL_NODE = 0;
  public static int LEAF = 1;
  public static int PRUNED_NODE = 2;
  
  public static void dfsWriteQT(QTNode node, PrintWriter writer) {
    if (node == null) {
      writer.println(PRUNED_NODE);
      return;
    }

    if (node.children == null) {
      writer.printf("%d %s", LEAF, node.id.equals("") ? "root" : node.id);
      for (int i = 0; i < DIM; ++i) {
        writer.printf(" %.10f %.10f", node.lo[i], node.hi[i]);
      }
      writer.println();
      return;
    }

    writer.printf("%d %s", INTERNAL_NODE, node.id.equals("") ? "root" : node.id);
    for (int i = 0; i < DIM; ++i) {
      writer.printf(" %.10f %.10f", node.lo[i], node.hi[i]);
    }
    writer.println();
    for (QTNode child : node.children) {
      dfsWriteQT(child, writer);
    }
  }
  
  public static void writeQT(QTNode root, String filename) throws Exception {
    PrintWriter writer =
        new PrintWriter(new BufferedWriter(new FileWriter(filename)));
    dfsWriteQT(root, writer);
    writer.flush();
    writer.close();
  }

  public static QTNode dfsReadQT(int depth, BufferedReader reader) throws Exception {
    String line = reader.readLine(); // should not be null
    Scanner sc = new Scanner(line);
    
    int cmd = sc.nextInt();
    if (cmd == PRUNED_NODE) return null;
    
    String id = sc.next();
    if (id.equals("root")) id = "";
    
    double[] lo = new double[DIM];
    double[] hi = new double[DIM];
    for (int i = 0; i < DIM; ++i) {
      lo[i] = sc.nextDouble();
      hi[i] = sc.nextDouble();
    }
    
    QTNode ret = new QTNode(depth, id, null, lo, hi);
    if (cmd == LEAF) {
      return ret;
    }
    
    // internal node
    ret.children = new QTNode[TWO_TO_THE_POWER_DIM];
    for (int chi = 0; chi < TWO_TO_THE_POWER_DIM; ++chi) {
      ret.children[chi] = dfsReadQT(depth + 1, reader);
    }
    
    return ret;
  }
  
  public static QTNode readQT(String filename) throws Exception {
    BufferedReader reader =
        new BufferedReader(new FileReader(new File(filename)));
    return dfsReadQT(0, reader);
  }
	private static BufferedReader br;
	
//	public static Path quad_tree_data_path;

  public int run(String[] args) throws Exception {
    /*
      - read min-max file
      - sampling
      - build QT
      - local MR
      - global MR
    */
//    br = new BufferedReader(new InputStreamReader(System.in));
    br = new BufferedReader(new FileReader(new File("data5000.txt")));
    
    Vector<Point> points = new Vector<Point>();
    String line;
    while ((line = br.readLine()) != null) {
      Point p = new Point();
      if (!p.parseFromRawLine(line)) continue;
      points.add(p);
    }
    
    double[] lo = {-110.0, -9999.9, -9999.9, -9999.9,     600.3,   0.0,   1.0,   1.0, -119.4};
    double[] hi = { 113.0,   119.0,  -901.3,   108.6,    9999.9, 999.9, 999.9, 999.9, 9999.9};
    QTNode root = new QTNode(0, "", points, lo, hi);
    
    root.divide(20);
    
    System.out.println("Quad tree division done.");
    
    writeQT(root, "quad_tree_data.out");
    
    System.out.println("Quad tree written.");
    
//    writeQT(root, "qt1.out");
//    QTNode root2 = readQT("qt1.out");
//    writeQT(root2, "qt2.out");
    
    /////////////
    Configuration config = getConf();
    FileSystem dfs = FileSystem.get(config);
    
//    System.out.println(dfs.getWorkingDirectory());

    //quad_tree_data_path = new Path("quad_tree_data"); // dfs.getWorkingDirectory()+"/TestDirectory/subDirectory/");
//    dfs.copyFromLocalFile(new Path("quad_tree_data.out"), new Path("qtd")); 

//    System.out.println("Quad tree copied.");
    
    JobConf conf = new JobConf(getConf(), Skyline.class);
    
//    DistributedCache.addCacheFile(new URI("qtd"), conf);
    
    Path hdfsPath = new Path("/user/cloudera/data/qtd.txt");

    // upload the file to hdfs. Overwrite any existing copy.
    dfs.copyFromLocalFile(false, true, new Path("quad_tree_data.out"), hdfsPath);
    DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
    
    conf.setJobName("Skyline");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(LSkyMapper.class);
    conf.setCombinerClass(LSkyReducer.class);
    conf.setReducerClass(LSkyReducer.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setJarByClass(Skyline.class);
    conf.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    MultipleOutputs.addNamedOutput(conf, "vpn", TextOutputFormat.class, Text.class, Text.class);
    MultipleOutputs.addNamedOutput(conf, "filter", TextOutputFormat.class, Text.class, Text.class);

    JobClient.runJob(conf);
    return 0;
  }

	public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Skyline(), args);
    System.exit(res);
  }
}

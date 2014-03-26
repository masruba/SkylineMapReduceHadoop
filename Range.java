//Find the max and min value range for each dimension using map reduce

package org.myorg;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.DistributedCache;

public class Range{

  public static final int[] pk_positions = {0,6,  14,22};  // [begin1, end1), [begin2, end2) : STN, YYYYMMDD
  public static final int[] value_positions = {24,30,  35,41,  46,52,  102,108,     57,63,  78,83,  88,93,  95,100,  110,116};
  public static final int[] value_type = {-1, -1, -1, -1,  1, 1, 1, 1, 1};  // -1 = maximize, +1 = minimize.
  public static final String[] missing = {"9999.9", "9999.9", "9999.9", "9999.9",    "9999.9", "999.9", "999.9", "999.9", "9999.9"};

  public static class Point {
    public int[] pk;
    public double[] value;
    public boolean[] mm;

    public Point() {
      pk = new int[pk_positions.length / 2];
      value = new double[value_positions.length / 2];
      mm = new boolean[value_positions.length / 2];
    }

    public boolean parseFromRawLine(String line) {  // When scanning raw input file in mapper of L-SKY-MR.
      // Read PK
      for (int i = 0; i < pk_positions.length; i += 2) {
        pk[i/2] = Integer.parseInt(line.substring(pk_positions[i], pk_positions[i+1]));
      }

      // Read value
      boolean flag = true;
      for (int i = 0; i < value_positions.length; i += 2) {
        // value_type will reverse the values.
        String substr = line.substring(value_positions[i], value_positions[i+1]);
        mm[i/2] = substr.equals(missing[i/2]);
        if (mm[i/2]) flag = false;
        
        value[i/2] = value_type[i/2] * Double.parseDouble(substr);
      }
      return flag;
    }
  }
  
    
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    private static final DoubleWritable one = new DoubleWritable(1.0);
    private static final Text ctext = new Text("c");
    private static final Text cnmtext = new Text("c_no_missing");
    
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
  		String line = value.toString();
  		if (line.trim().equals("")) {
  		  return;
  		}
 		
      output.collect(ctext, one);

  		Point p = new Point();
  		if (p.parseFromRawLine(line)) {
  		  output.collect(cnmtext, one);
  		}
  		
  		int i = 0;
      for (double v : p.value) {
        if (!p.mm[i]) {
          output.collect(new Text(Integer.toString(i)), new DoubleWritable(v));
          output.collect(new Text(String.format("c%d",i)), one);
        }
		    ++i;
      }
    }
  }  
 
  public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
      
      if (key.toString().charAt(0) == 'c') {
        double c = 0.0;
		    while (values.hasNext()) {
		      c += values.next().get();
		    }
		    output.collect(key, new DoubleWritable(c));
		    return;
		  }

      double maxValue = -Double.MAX_VALUE;
		  double minValue = Double.MAX_VALUE;
		  
		  while (values.hasNext()) {
			  double v = values.next().get();				
			  if (v > maxValue) {
			    maxValue = v;
			  }
			  if (v < minValue) {
          minValue = v;
        }
		  }

		  output.collect(key, new DoubleWritable(maxValue));    		  		
		  output.collect(key, new DoubleWritable(minValue)); 

/*
      double maxValue = -Double.MAX_VALUE;
		  double minValue = Double.MAX_VALUE;
      double maxValue2 = -Double.MAX_VALUE;
		  double minValue2 = Double.MAX_VALUE;
		  
		  while (values.hasNext()) {
			  double v = values.next().get();				
			  if (v > maxValue) {
			    maxValue2 = maxValue;
			    maxValue = v;
			  } else if (v > maxValue2) {
			    maxValue2 = v;
			  }
			  
			  if (v < minValue) {
          minValue2 = minValue;
          minValue = v;
        } else if (v < minValue2) {
          minValue2 = v;
        }
		  }

		  output.collect(key, new DoubleWritable(maxValue));    		
		  output.collect(key, new DoubleWritable(maxValue2));    		
		  output.collect(key, new DoubleWritable(minValue2));    		
		  output.collect(key, new DoubleWritable(minValue)); 		*/
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Range.class);
    conf.setJobName("Range");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(DoubleWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setJarByClass(Range.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);

  } 
}


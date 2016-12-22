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

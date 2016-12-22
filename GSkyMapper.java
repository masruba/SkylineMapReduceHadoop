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
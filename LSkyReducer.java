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

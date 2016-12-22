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

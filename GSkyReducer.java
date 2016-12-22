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
//        output.collect(empty_key, new Text(p.formatPK()));
    } 

    for (Point pd : star_points) {
      output.collect(key, new Text("*" + pd.toString()));
    }
  }
}

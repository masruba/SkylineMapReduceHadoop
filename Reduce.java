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
    output.collect(key, new DoubleWritable(minValue));    */
  }
}

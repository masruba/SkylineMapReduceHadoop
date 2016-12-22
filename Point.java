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

  // When scanning raw input file in mapper of L-SKY-MR.
  public boolean parseFromRawLine(String line) {  
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

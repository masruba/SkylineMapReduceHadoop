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
}

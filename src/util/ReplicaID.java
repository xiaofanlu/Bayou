package util;

/**
 * Created by xiaofan on 4/13/15.
 */
public class ReplicaID {
  public int acceptTime;
  public ReplicaID parent;
  public int pid;

  /**
   * The very first replica
   */
  public ReplicaID (int id) {
    acceptTime = 0;
    parent = null;
    pid = id;
  }

  public ReplicaID(int acTime, ReplicaID rid, int id) {
    acceptTime = acTime;
    parent = rid;
    pid = id;
  }

  public String toString () {
    if (parent == null) {
      return "rid(root)";
    } else {
      return "rid(" + acceptTime + ", " + parent + " )";
    }
  }
}

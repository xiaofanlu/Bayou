package util;

import java.io.Serializable;

/**
 * Created by xiaofan on 4/13/15.
 */
public class ReplicaID implements Serializable {
  public static final long serialVersionUID = 4241233L;

  public int acceptTime;
  public ReplicaID parent;
  public int pid;

  /**
   * root id
   * @return
   */
  public ReplicaID() {
    acceptTime = -1;
    parent = null;
    pid = -1;
  }

  /**
   * The very first replica
   */
  public ReplicaID (int id) {
    acceptTime = 0;
    parent = new ReplicaID();
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
      return "rid(" + acceptTime + ", " + parent + ")";
    }
  }
}

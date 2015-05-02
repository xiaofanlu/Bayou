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
  
  public ReplicaID(ReplicaID rid){
	  this.acceptTime = rid.acceptTime;
	  if(rid.parent == null){
		  this.parent = null;
	  }else{
		  this.parent = new ReplicaID(rid.parent);
	  }
	  this.pid = rid.pid;
  }

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
      return "rid(" + acceptTime + ", " + parent + ")";
    }
  }
}

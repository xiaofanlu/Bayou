package command;

import util.ReplicaID;

/**
 * Created by xiaofan on 4/13/15.
 */

public class Create extends ServerCmd {
  public Create(ReplicaID rid, int acptTime) {
    super(rid, acptTime);
  }

  public String name() {
    return "Create";
  }
}
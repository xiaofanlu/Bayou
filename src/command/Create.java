package command;

import util.ReplicaID;

/**
 * Created by xiaofan on 4/13/15. This command is sent from a existing replica to the new created 
 * replica. rid here is the rid of the new replica. 
 */

public class Create extends ServerCmd {
  public Create(ReplicaID rid, int acptTime) {
    super(rid, acptTime);
  }

  public String name() {
    return "Create";
  }
}
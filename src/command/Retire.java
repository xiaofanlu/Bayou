package command;

import util.ReplicaID;

/**
 *  replica retires by sending retirement write to itself
 *  must remain alive until at least one anti-entropy step.
 *  rid here is the id of the retiring server. Once the write
 *  containing Retire command is sent to other server, the retiring
 *  server retires.
 */
public class Retire extends ServerCmd {
  public Retire(ReplicaID rid, int acptTime) {
    super(rid, acptTime);
  }

  public String name () {
    return "Retire";
  }
}

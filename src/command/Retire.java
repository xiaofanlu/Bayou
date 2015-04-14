package command;

import util.ReplicaID;

/**
 *  replica retires by sending retirement write to itself
 *  must remain alive until at least one anti-entropy step.
 */
public class Retire extends ServerCmd {
  public Retire(ReplicaID rid, int acptTime) {
    super(rid, acptTime);
  }

  public String name () {
    return "Retire";
  }
}

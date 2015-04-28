package command;

import util.ReplicaID;

/**
 * Created by xiaofan on 4/13/15.
 */
public class ServerCmd extends Command {
  public ReplicaID rid;
  public int acceptTime;

  public ServerCmd(ServerCmd cmd){
	  this.rid = new ReplicaID(cmd.rid);
	  this.acceptTime = cmd.acceptTime;
  }
  
  public ServerCmd (ReplicaID rid, int acptTime) {
    this.rid = rid;
    acceptTime = acptTime;
  }

  public String toString() {
    return  name() + "(" + rid + ")";
  }

  public String name () {
    return "cmd";
  }
}

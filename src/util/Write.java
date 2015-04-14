package util;

import command.Command;

/**
 * Created by xiaofan on 4/13/15.
 */
public class Write implements Comparable<Write> {
  public int csn;
  public int acceptTime;
  public ReplicaID replicaId;
  public Command command;

  public Write(int commitSequenceNumber, int time, ReplicaID rid, Command cmd) {
    csn = commitSequenceNumber;
    acceptTime = time;
    replicaId = rid;
    command = cmd;
  }

  @Override
  public int compareTo(Write other) {
    if (this.csn != other.csn) {
      return this.csn - other.csn;
    } else if (this.acceptTime != other.acceptTime) {
      return this.acceptTime - other.acceptTime;
    } else {
      // todo
      return 0;
    }
  }


  public boolean sameAs(Write other) {
    return replicaId.toString().equals(other.replicaId.toString()) &&
        acceptTime == other.acceptTime;
  }
}

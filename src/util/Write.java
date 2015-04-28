package util;

import command.Command;

import java.io.Serializable;

/**
 * Write: log entry
 */
public class Write implements Comparable<Write>, Serializable {
  public static final long serialVersionUID = 4241231L;

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
  
  public Write(Write wr){
	  this.csn = wr.csn;
	  this.acceptTime = wr.acceptTime;
	  this.replicaId = new ReplicaID(wr.replicaId);
	  this.command = new Command(wr.command);
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

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("W(");
    sb.append(csn);
    sb.append(", ");
    sb.append(acceptTime);
    sb.append(", ");
    sb.append(replicaId.toString());
    sb.append(") ");
    return sb.toString();
  }
}

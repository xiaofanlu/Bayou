package msg;

import util.ReplicaID;

/**
 * Created by xiaofan on 4/13/15.
 */
public class CreateReplyMsg extends Message {
  public ReplicaID rid;

  public CreateReplyMsg(int src, int dst, ReplicaID newId) {
    super(src, dst);
    rid = newId;
  }

  public String toString () {
    return super.toString() + "CreateReply(" + rid + ")";
  }
}

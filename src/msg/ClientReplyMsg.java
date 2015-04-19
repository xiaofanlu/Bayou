package msg;

import command.ClientCmd;
import command.Del;
import command.Get;
import command.Put;
import util.Write;

/**
 * Reply for client message from server
 */


public class ClientReplyMsg extends Message {
  public boolean suc;
  public ClientCmd cmd;
  public Write write;
  public String url;

  /**
   * failure write/read
   */
  public ClientReplyMsg(ClientMsg msg) {
    super(msg.dst, msg.src);
    suc = false;
    cmd = msg.cmd;
    write = null;
    url = "";
  }

  /**
   * success write
   */
  public ClientReplyMsg(ClientMsg msg, Write w) {
    this(msg);
    suc = true;
    write = w;
  }

  /**
   * success read
   */
  public ClientReplyMsg(ClientMsg msg, String u, Write w) {
    this(msg);
    suc = true;
    url = u;
    write = w;
  }

  public String toString() {
    String rst = super.toString();
    if (cmd instanceof Put) {
      rst += "PutReply(" + suc + ", " + write + ")";
    } else if (cmd instanceof Del) {
      rst += "DelReply(" + suc + ", " + write + ")";
    } else if (cmd instanceof Get) {
      rst+= "GetReply(" + suc + ", " + url + ", " + write + ")";
    } else {
      rst += "";
    }
    return rst;
  }
}

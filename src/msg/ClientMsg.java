package msg;

import command.ClientCmd;
import command.Del;
import command.Get;
import command.Put;
import util.SessionManager;

/**
 * Client Request Message
 */
public class ClientMsg extends Message {
  public ClientCmd cmd;
  public SessionManager sm;

  public ClientMsg(int src, int dst, ClientCmd cmd, SessionManager sm) {
    super(src, dst);
    this.cmd = cmd;
    this.sm = sm;
  }

  public String toString () {
    return super.toString() + "ClientRequest(" + cmd + ")";
  }

  public boolean isWrite() {
    return cmd instanceof Put || cmd instanceof Del;
  }

  public boolean isRead() {
    return cmd instanceof Get;
  }
}

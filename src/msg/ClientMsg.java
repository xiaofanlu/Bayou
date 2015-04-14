package msg;

import command.ClientCmd;

/**
 * Created by xiaofan on 4/13/15.
 */
public class ClientMsg extends Message {
  public ClientCmd cmd;

  public ClientMsg(int src, int dst, ClientCmd cmd) {
    super(src, dst);
    this.cmd = cmd;
  }

  public String toString () {
    return super.toString() + "ClientRequest(" + cmd + ")";
  }
}

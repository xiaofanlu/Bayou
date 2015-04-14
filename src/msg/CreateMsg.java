package msg;

import command.Create;

/**
 * Created by xiaofan on 4/13/15.
 */
public class CreateMsg extends Message {
  public Create cmd;

  public CreateMsg(int src, int dst, Create cmd) {
    super(src, dst);
    this.cmd = cmd;
  }

  public String toString () {
    return super.toString() + "Create(" + cmd + ")";
  }
}

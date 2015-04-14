package msg;

import command.Command;

/**
 * Created by xiaofan on 4/13/15.
 */
public class RequestReply extends Message {
  public Command command;

  public RequestReply(int s, int d) {
    super(s, d);
  }
}

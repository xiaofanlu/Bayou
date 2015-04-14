package command;

/**
 * Created by xiaofan on 4/13/15.
 */
public class Put extends ClientCmd {
  public Put(String s, String u) {
    super(s, u);
  }

  public String toString () {
    return "Put(" + song + "@" + url + ")";
  }
}

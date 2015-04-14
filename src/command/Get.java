package command;

/**
 * Created by xiaofan on 4/13/15.
 */

public class Get extends ClientCmd {

  public Get(String s) {
    super(s);
  }

  public String toString() {
    return "Get(" + song + ")";
  }
}

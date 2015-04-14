package command;

/**
 * Created by xiaofan on 4/13/15.
 */

public class Del extends ClientCmd {

  public Del(String s) {
    super(s);
  }

  public String toString() {
    return "Del(" + song + ")";
  }
}
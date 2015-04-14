package msg;

import java.io.Serializable;

/**
 * Created by xiaofan on 4/13/15.
 */
public class Message implements Serializable {
  public int src;
  public int dst;

  public Message (int s, int d) {
    src = s;
    dst = d;
  }

  public String toString() {
    return src + " -> " + dst + ": ";
  }

}


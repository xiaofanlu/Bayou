package msg;

import java.io.Serializable;

/**
 * Super Message class
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

  public boolean isAEmsg () {
    return false;
  }
  /*
  public boolean isAEmsg () {
    return this instanceof AEAckMsg  ||
           this instanceof AERplyMsg ||
           this instanceof AERqstMsg;
  }
  */

}


package msg;

import util.Write;

/**
 * third of the three message for anti-entropy process
 * Ack with the writes the receiver don't know about
 */
public class AEAckMsg extends Message {
  public Write write;
  /*should R commit the known write or not */
  public boolean commit;

  public AEAckMsg(int s, int d, Write w) {
    super (s, d);
    write = w;
    commit = false;
  }
  public AEAckMsg(int s, int d, Write w, boolean c) {
    super (s, d);
    write = w;
    commit = c;
  }

  public String toString() {
    return super.toString() + " Anti_entropy_ack: "
            + write.toString() + " Commit?" + commit;
  }

}

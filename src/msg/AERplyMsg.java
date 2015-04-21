package msg;

import util.VersionVector;

/**
 * second of the three message for anti-entropy process
 * Reply with current versionVector and highest committed sequence number
 */
public class AERplyMsg extends Message {
  public VersionVector vv;
  public int CNS;

  public AERplyMsg(int s, int d, VersionVector vector, int cns) {
    super(s, d);
    vv = vector;
    CNS = cns;
  }

  public boolean hasKey(String rid) {
    return vv.hasKey(rid);
  }

  public int getTime(String rid) {
    return vv.get(rid);
  }

  public String toString() {
    return super.toString() + "Anti_entropy_reply(" + CNS + ")";
  }
}

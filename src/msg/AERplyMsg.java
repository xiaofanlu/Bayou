package msg;

import java.util.Map;

/**
 * second of the three message for anti-entropy process
 * Reply with current versionVector and highest committed sequence number
 */
public class AERplyMsg extends Message {
  public Map<String, Integer> versionVector;
  public int CNS;

  public AERplyMsg(int s, int d, Map<String, Integer> vv, int cns) {
    super(s, d);
    versionVector = vv;
    CNS = cns;
  }

  public String toString() {
    return super.toString() + "Anti_entropy_reply()";
  }
}

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

  public boolean hasKey(String rid) {
    return versionVector.containsKey(rid);
  }

  public int getTime(String rid) {
    assert versionVector.containsKey(rid);
    return versionVector.get(rid);
  }

  public String toString() {
    String ans = super.toString() + "Anti_entropy_reply(" + CNS + ")";
    for(String s : versionVector.keySet()){
    	ans = ans + "\n" + "(" + s + ", "+versionVector.get(s) + ")"; 
    }
    return ans;
  }
  @Override
  public boolean isAEmsg(){
	  return true;
  }
}

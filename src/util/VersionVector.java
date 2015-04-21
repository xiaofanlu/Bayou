package util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Version Vector, a.k.a Vector Clock
 */
public class VersionVector implements Serializable {

  public static final long serialVersionUID = 4241230223L;
  Map<String, Integer> vector = new HashMap<String, Integer>();

  public boolean hasKey(String rid) {
    return vector.containsKey(rid);
  }

  public int get(String rid) {
    assert vector.containsKey(rid);
    return vector.get(rid);
  }

  public void put(String rid, int time) {
    vector.put(rid, time);
  }

  public void remove(String rid) {
    assert vector.containsKey(rid);
    vector.remove(rid);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    Map<String, Integer> map = new HashMap<String, Integer>(vector);
    sb.append("\n================Version Vector==============\n");
    for (String key : map.keySet()) {
      sb.append(key);
      sb.append(": ");
      sb.append(map.get(key));
      sb.append("\n");
    }
    sb.append("==================    End     ==============\n");
    return sb.toString();
  }
}

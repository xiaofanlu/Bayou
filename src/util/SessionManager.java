package util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * SessionManager as specified in
 * "Session Guarantees for Weakly Consistent Replicated Data"
 *
 *
 */
public class SessionManager implements Serializable {
  public static final long serialVersionUID = 4241239L;

  public Map<String, Integer> readVector = new HashMap<String, Integer> ();
  public Map<String, Integer> writeVector = new HashMap<String, Integer> ();
  
  /* YW: need to record replicaId to decide whether the servers of previous read/write is n
   * never seen or the server is already retired*/
  
  public Map<String, ReplicaID> readReplicaIdMap = new HashMap<String, ReplicaID> ();
  public Map<String, ReplicaID> writeReplicaIdMap = new HashMap<String, ReplicaID> ();
  
  /**
   * Update Write Vector based on reply for Write
   * @param w Write entry
   */
  public void updateWrite(Write w) {
    writeVector.put(w.replicaId.toString(), w.acceptTime);
    ReplicaID tempId = new ReplicaID(w.replicaId);
    writeReplicaIdMap.put(tempId.toString(), tempId);
  }


  /**
   * Update Read Vector (the Writes relevant to latest Read)
   * based on reply
   * @param w Write entry
   */
  public void updateRead(Write w) {
    readVector.put(w.replicaId.toString(), w.acceptTime);
    ReplicaID tempId = new ReplicaID(w.replicaId);
    readReplicaIdMap.put(tempId.toString(), tempId);
  }

  /**
   * Check ReadVector and WriteVector against Version Vector on server
   * @param vv Version Vector
   * @return whether Version Vector dominates both Read/Write Vector
   */
  public boolean isDominatedBy(Map<String, Integer> vv) {
    //return dominates(vv, readVector) && dominates(vv, writeVector);
    /* YW: Use complete version vector instead*/
	  return completeDominates(vv, readVector, readReplicaIdMap) && 
			  completeDominates(vv, writeVector, writeReplicaIdMap);
  }

  /**
   * Whether vector a dominates vector b
   * @param supV : super vector
   * @param subV : sub vector
   * @return
   */
  /*public boolean dominates(Map<String, Integer> supV,
                           Map<String, Integer> subV ) {
    for (String rid : subV.keySet()) {
      if (!supV.containsKey(rid)) {
        if (subV.get(rid) != 0) {
          return false;
        }
      } else {
        if (supV.get(rid) < subV.get(rid)) {
          return false;
        }
      }
    }
    return true;
  }
  */
  
  /**
   * YW: completeDominates
   * @param supV : sup vector
   * @param subV : sub vector
   * @param map : mapping from subV's replica id string to ReplicaID
   * @return supV dominates subV
   */
  public boolean completeDominates(Map<String, Integer> supV,
  									Map<String, Integer> subV,
  									Map<String, ReplicaID> map){
	  for(String rid : subV.keySet()){
		  Integer completeVersion = sessionCompleteV(supV, map.get(rid));
		  if(completeVersion < subV.get(rid)){
			  return false;
		  }
	  }
	  return true;
  }
  
  /**
   * YW: Helper function, CompleteV
   */
  public Integer sessionCompleteV(Map<String, Integer> vv, ReplicaID rid){
	  if(vv.containsKey(rid.toString())){
		  return vv.get(rid.toString());
	  }else if(rid.parent == null){
		  return Integer.MAX_VALUE;
	  }else{
		  if(sessionCompleteV(vv, rid.parent) >= rid.acceptTime){
			  return Integer.MAX_VALUE;
		  }else{
			  return Integer.MIN_VALUE;
		  }
	  }
  }
}

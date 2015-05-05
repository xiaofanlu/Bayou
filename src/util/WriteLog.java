package util;

import command.ClientCmd;
import command.Del;
import command.Put;
import command.Retire;
import command.Create;

import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.ArrayList;
import java.util.List;

/**
 * Log of all the Writes on a Server
 * 1. ordered by CSN..
 * 2. commmitable
 */
public class WriteLog {
  //PriorityQueue<Write> log;
  List<Write> ywLog;
  boolean debug = Constants.debug;

  public WriteLog() {
    //log = new PriorityQueue<Write>();
	  ywLog = new ArrayList<Write>();
  }

  public Iterator<Write> getIterator () {
    //return new PqIter(log);
    return new listIter(ywLog);
  }

  public void add (Write entry) {
    //log.offer(entry);
	  Iterator<Write> iter = getIterator();
	  Write insertWrite = null; // the first write larger than entry
	  Write wr = null;
	  while(iter.hasNext()){
		  wr = iter.next();
		  if(wr.compareTo(entry) <= 0){
			  // no larger than entry, don't insert entry
		  }else{
			  insertWrite = wr;
			  break;
		  }
	  }
	  if(insertWrite == null){
		  ywLog.add(entry);
	  }else{
		  int location = ywLog.indexOf(insertWrite);
		  ywLog.add(location, entry);
	  }
  }

  /**
   * Commit a tentative write
   * remove from original location, insert again.
   * @param entry
   * @return
   */
  public boolean commit (Write entry) {
    Iterator<Write> it = getIterator();
    while (it.hasNext()) {
      Write w = it.next();
      if (w.sameAs(entry) && w.csn != entry.csn) {
    	  ywLog.remove(w);
    	  add(entry);
        //log.remove(w);
        //log.offer(entry);
        return true;
      }
    }
    return false;
  }

  /**
   * Get last relevant write for a Read command
   * todo: update datastructure to doublely linked list
   */
  public Write lastRelevantWrite(String song) {
    Write last = null;
    Iterator<Write> it = getIterator();
    while (it.hasNext()) {
      Write cur = it.next();
      if (cur.command instanceof ClientCmd) {
        String name = ((ClientCmd)cur.command).song;
        if (name.equals(song)) {
           last = cur;
        }
      }
    }
    return last;
  }

  public void print() {
    Iterator<Write> it = getIterator();
    while (it.hasNext()) {
      Write cur = it.next();
      if (cur.command instanceof Put) {
        Put cmd = (Put) cur.command;
        System.out.print("PUT:(");
        System.out.print(cmd.song + ", " + cmd.url);
      } else if (cur.command instanceof Del) {
        Del cmd = (Del) cur.command;
        System.out.print("DELETE:(");
        System.out.print(cmd.song);
      } else if (cur.command instanceof Create){
    	  Create cmd = (Create) cur.command;
    	  if(debug){
    		  System.out.print("Create " + cmd.rid.toString());
    	  }
      } else if (cur.command instanceof Retire){
    	  Retire cmd = (Retire) cur.command;
    	  if(debug){
    		  System.out.print("Retire " + cmd.rid.toString());
    	  }
      }
      else {
        System.out.print("UNKNOWN:(");
      }
      if(cur.command instanceof Put || cur.command instanceof Del){
    	  System.out.println("):" + (cur.csn == Integer.MAX_VALUE ?
                                                  "FALSE" : "TRUE"));
      }else if(debug){
    	  System.out.println("):" + (cur.csn == Integer.MAX_VALUE ?
                  "FALSE" : "TRUE"));
      }
    }
  }

  public Write getLast(){// return the last entry in WriteLog
	    Iterator<Write> it = getIterator();
	    Write last = null;
	    while(it.hasNext()){
	    	last = it.next();
	    }
	    return last;
  }

  /**
   * Build iterator on a new copy of queue
   */
  class PqIter implements Iterator<Write> {
    final PriorityQueue<Write> pq;
    public PqIter(PriorityQueue <Write> source) {
      pq = new PriorityQueue(source);
    }

    @Override
    public boolean hasNext() {
      return pq.peek() != null;
    }

    @Override
    public Write next() {
      return pq.poll();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("");
    }
  }
  
  /**
   * Iterator over ArrayList
   */
  class listIter implements Iterator<Write>{
	  final List<Write> list;
	  public listIter(List<Write> source){
		  list = new ArrayList<Write>(source);
	  }
	  @Override
	  public boolean hasNext(){
		  return !list.isEmpty();
	  }
	  @Override
	  public Write next(){
		  return list.remove(0);
	  }
	  @Override
	  public void remove(){
		  throw new UnsupportedOperationException("");
	  }
  }
}

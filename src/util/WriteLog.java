package util;

import command.ClientCmd;
import command.Del;
import command.Put;

import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Log of all the Writes on a Server
 * 1. ordered by CSN..
 * 2. commmitable
 */
public class WriteLog {
  PriorityQueue<Write> log;

  public WriteLog() {
    log = new PriorityQueue<Write>();
  }

  public Iterator<Write> getIterator () {
    return new PqIter(log);
  }

  public void add (Write entry) {
    log.offer(entry);
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
        log.remove(w);
        log.offer(entry);
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
      } else {
        System.out.print("UNKNOWN:(");
      }
      System.out.println("):" + (cur.csn == Integer.MAX_VALUE ?
                                                  "TRUE" : "FALSE"));
    }
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
}

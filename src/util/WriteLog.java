package util;

import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Created by xiaofan on 4/13/15.
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

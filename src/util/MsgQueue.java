package util;

import msg.Message;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by xiaofan on 3/30/15.
 * ref: http://tutorials.jenkov.com/java-concurrency/blocking-queues.html
 */
public class MsgQueue {
  private List<Message> queue = new LinkedList<Message>();
  private int limit;

  public MsgQueue() {
    limit = 100000;
  }

  public synchronized void offer(Message msg) {
    while (this.queue.size() == this.limit) {
      waitQ();
    }
    if (this.queue.size() == 0) {
      notifyAll();
    }
    this.queue.add(msg);
  }

  public synchronized Message poll() {
    while (this.queue.size() == 0) {
      waitQ();
    }
    if (this.queue.size() == this.limit) {
      notifyAll();
    }

    return this.queue.remove(0);
  }


  public void waitQ() {
    try {
      wait();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}

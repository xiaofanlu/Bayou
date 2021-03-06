package exec;

import framework.Config;
import framework.NetController;
import org.apache.commons.codec.binary.Base64;
import util.Constants;
import msg.Message;
import util.MsgQueue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

/**
 * Provide communication abstraction for client/servers
 */


public class NetNode extends Thread {
  public boolean debug = Constants.debug;
  public boolean shutdown = false;

  private volatile boolean pause = false;

  public int pid;
  MsgQueue inbox;

  Config config;
  NetController nc;


  public NetNode(int id) {
    pid = id;
    inbox = new MsgQueue();

    config = new Config(id, Constants.MAX_NODE);
    nc = new NetController(config);
    new Listener().start();
  }

  public void pause() {
    pause = true;
  }

  /**
   * can't use start() as it is reserved for Thread start()
   */
  public void unPause() {
    pause = false;
  }

  /**
   * Send message
   *
   * @param msg
   */
  public void send(Message msg) {
    if (debug) {
      print("Sent: " + msg.toString());
    }
    if (pause && msg.isAEmsg()) {
      return;
    }
    nc.sendMsg(msg.dst, serialize(msg));
  }

  public void deliver(Message msg) {
    if (pause && msg.isAEmsg()) {
      return;
    }
    inbox.offer(msg);
  }

  public Message receive() {
    Message msg = inbox.poll();
    return msg;
  }


  public void print(String msg) {
    System.out.println(name() + " " + pid + ": " + msg);
  }

  public String name() {
    return "Node";
  }

  /**
   * Translate the Message to a string to transmit through socket
   * Don't want to modify existing socket framework
   */
  public String serialize(Message msg) {
    String rst = "";
    try {
      ByteArrayOutputStream bo = new ByteArrayOutputStream();
      ObjectOutputStream so = new ObjectOutputStream(bo);
      so.writeObject(msg);
      so.flush();

      //rst = bo.toString();
      rst = new String(Base64.encodeBase64(bo.toByteArray()));
    } catch (Exception e) {
      System.out.println(e);
    }
    return rst;
  }

  /**
   * Translate String to a Message upon receiving from socket
   * Don't want to modify existing socket framework
   */
  public Message deserialize(String str) {
    Message msg = null;
    try {
      //byte b[] = str.getBytes();
      //byte b[] = str.getBytes("ISO-8859-1");
      byte b[] = Base64.decodeBase64(str.getBytes());
      ByteArrayInputStream bi = new ByteArrayInputStream(b);
      ObjectInputStream si = new ObjectInputStream(bi);
      msg = (Message) si.readObject();
    } catch (Exception e) {
      System.out.println(e);
    }
    return msg;
  }


  /**
   * Inner listener thread
   */
  class Listener extends Thread {
    public void run() {
      while (!shutdown) {
        List<String> buffer = nc.getReceivedMsgs();
        for (String str : buffer) {
          Message msg = deserialize(str);
          if (msg != null) {
            deliver(msg);
          }
        }
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
}

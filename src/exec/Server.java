package exec;

import command.*;
import msg.*;
import util.*;

import java.util.*;

/**
 * Server/Replica class
 */
public class Server extends NetNode {
  int timeStamp = 1;
  int csnStamp = 1;
  boolean isPrimary = false;

  int maxCSN = 0;


  ReplicaID rid = null;
  ArrayList<Integer> connected = new ArrayList<Integer>();
  PlayList playList = new PlayList();
  WriteLog writeLog = new WriteLog();

  VersionVector vv = new VersionVector();


  boolean toRetire = false;
  boolean retired = false;

  /**
   * Primary server, start with id = 0
   * @param id
   */
  public Server(int id) {
    super(id);
    rid = new ReplicaID(id);
    isPrimary = true;
    start();
  }

  /**
   * Non-primary server, no need to remember the primary ID.
   * @param id
   * @param primary
   */
  public Server(int id, int primary) {
    super(id);
    start();
    send(new CreateMsg(pid, primary));
  }


  public String name () {
    return "Server";
  }

  public boolean isPrimary() {
    return isPrimary;
  }

  public int nextTimeStamp () {
    if (debug) {
      print("nextTimeStamp: " + timeStamp);
    }
    return ++timeStamp;
  }

  public int currTimeStamp () {
    return timeStamp;
  }

  public int getCSN() {
    maxCSN = csnStamp;
    if (debug) {
      print("Get new CSN: " + csnStamp);
    }
    return csnStamp++;
  }

  /**
   * Main Server Thread, message handler
   */
  public void run() {
    if (debug) {
      print("Started");
    }
    while (!retired){
      Message msg = receive();

      if (debug) {
        print("Received" + msg.toString());
      }
      /* request from client */
      if (msg instanceof ClientMsg) {
        ClientMsg rqst = (ClientMsg) msg;
        originalClientCmd(rqst);
      }

      /* create write from other connected server */
      else if (msg instanceof CreateMsg) {
        CreateMsg create = (CreateMsg) msg;
        originalCreateCmd(create);
      } else if (msg instanceof CreateReplyMsg) {
        CreateReplyMsg cReply = (CreateReplyMsg) msg;
        setUpServer(cReply);
      }

      /* anti-entropy messages */
      else if (msg instanceof AERqstMsg) {
        send(new AERplyMsg(pid, msg.src, vv, maxCSN));
      } else if (msg instanceof AERplyMsg) {
        AERplyMsg m = (AERplyMsg) msg;
        anti_entropy(m);
        if (toRetire) {
          retire(msg.src);
        }
      } else if (msg instanceof AEAckMsg) {
        AEAckMsg ackMsg = (AEAckMsg) msg;
        antiEntropyACKHandler(ackMsg);
      }

      /* primary handoff Message */
      else if (msg instanceof PrimaryHandOffMsg) {
        PrimaryHandOffMsg pho = (PrimaryHandOffMsg) msg;
        isPrimary = true;
        csnStamp = pho.curCSN;
      }
    }
  }


  /**
   * Initialization upon receiving reply from create write
   * @param cReply
   */
  public void setUpServer(CreateReplyMsg cReply) {
    if (debug) {
      print("Set up Server!");
    }
    rid = cReply.rid;
    timeStamp = rid.acceptTime + 1;
    vv.put(rid.toString(), 0);
  }


  /**
   *  initiate anti-entropy process with every connected node
   */
  public void doGossip() {
    for (int node : connected) {
      send(new AERqstMsg(pid, node));
    }
  }

  /**
   *  initiate anti-entropy process with every connected node
   *  Get the message from src, no need to send it back again
   */
  public void doGossip(int src) {
    for (int node : connected) {
      if (node != src) {
        send(new AERqstMsg(pid, node));
      }
    }
  }
  /**
   * Get write from client for the first time,
   * no need to roll-back as I am the first one to have this Write
   * 1. check domination
   * -- if write
   *   2. generate Write entry
   *   3. update writeLog
   *   4. update DB
   *   5. update versionVector
   *   6. send ack to client
   *   7. doGossip()
   * -- if read
   *   relevant read?
   */
  public void originalClientCmd (ClientMsg rqst) {
    if (!rqst.sm.isDominatedBy(vv)) {
      send(new ClientReplyMsg(rqst));
      return;
    }
    if (rqst.isWrite()) {
      if (playList.update(rqst.cmd)) {
        int acceptTime = nextTimeStamp();
        int csn = isPrimary() ? getCSN() : Integer.MAX_VALUE;
        Write write = new Write(csn, acceptTime, rid, rqst.cmd);
        writeLog.add(write);
        vv.put(rid.toString(), currTimeStamp());
        send(new ClientReplyMsg(rqst, write));
        doGossip();
      } else {
        send(new ClientReplyMsg(rqst));
      }
    } else if (rqst.isRead()) {
      String song = rqst.cmd.song;
      String url = playList.get(song);
      Write w = writeLog.lastRelevantWrite(song);
      send(new ClientReplyMsg(rqst, url, w));
    }
  }


  /**
   * Get Create from server for the first time,
   * no need to roll-back as I am the first one to have this Write
   *
   */
  public void originalCreateCmd (CreateMsg msg) {
    int acceptTime = nextTimeStamp();
    ReplicaID newId = new ReplicaID(acceptTime, rid, msg.src);
    int csn = isPrimary() ? getCSN() : Integer.MAX_VALUE;
    Create create = new Create(newId, acceptTime);
    Write entry = new Write(csn, acceptTime, rid, create);
    writeLog.add(entry);

    vv.put(rid.toString(), currTimeStamp());
    vv.put(newId.toString(), currTimeStamp() + 1);

    // send ack to server
    // some new server would ingore the first message,
    // send a dummy message here to refresh, no side effect
    send(new Message(pid, msg.src));
    send(new CreateReplyMsg(pid, msg.src, newId));
    // update with neighbors, anti-entropy
    doGossip();
  }


  /**
   * Propagating committed writes upon receiving reply from R
   * Complex logic, can be buggy
   */
  public void anti_entropy(AERplyMsg msg) {
    if (debug) {
      System.out.println("\n\n");
      System.out.println("\n\n");
      print("Anti_Entropy:");
      System.out.println("My VV: " + vv.toString());
      System.out.println("Server " + msg.src + "'s VV: " + msg.vv.toString());
      System.out.println(writeLog);

      System.out.println("\n\n");
      System.out.println("\n\n");
    }


    Iterator<Write> it = writeLog.getIterator();
    while (it.hasNext()) {
      Write w = it.next();
      String rjID = w.replicaId.toString();         // R_j, owner of the write
      String rkID = w.replicaId.parent.toString();  // R_k
      if (w.csn <= msg.CNS) {
        /* already committed in R */
        continue;
      }  else if (w.csn < Integer.MAX_VALUE) {   // > msg.CNS
        /*  committed write unknown to R */
        if (msg.hasKey(rjID)) {
          if ( w.acceptTime <= msg.getTime(rjID)) {
            /* R has the write, but doesn't know it is committed  */
            send(new AEAckMsg(pid, msg.src, w, true));
          } else {
            /* R don't have the write, add committed write  */
            send(new AEAckMsg(pid, msg.src, w));
          }
        } else {
          /*  the Missing VV entry, don't know of rjID ...  */
          int riVrk = msg.hasKey(rkID) ? msg.getTime(rkID) : -1;
          int TSkj = w.replicaId.acceptTime;
          if (riVrk < TSkj) {
            send(new AEAckMsg(pid, msg.src, w));
          }
        }
      } else {
        /* all tentative writes */
        if (msg.hasKey(rjID)) {
          if (msg.getTime(rjID) < w.acceptTime) {
            send(new AEAckMsg(pid, msg.src, w));
          }
        } else {
          /*  the Missing VV entry, don't know of rjID ...  */
          int riVrk = msg.hasKey(rkID) ? msg.getTime(rkID) : -1;
          int TSkj = w.replicaId.acceptTime;
          if (riVrk < TSkj) {
            send(new AEAckMsg(pid, msg.src, w));
          }
        }
      }
    }
  }


  /**
   * handler for write updates through anti-entropy process
   * @param ackMsg
   */
  public void antiEntropyACKHandler(AEAckMsg ackMsg) {
    if (ackMsg.commit) {
      /* I have the write, just need to commit it
       * rollback may be needed ...
       */
      if (writeLog.commit(ackMsg.write)) {
        /* successfully updated */
        maxCSN = Math.max(maxCSN, ackMsg.write.csn);
        refreshPlayList();
        doGossip();
      }
    } else {
      boolean newCommitted = false;
      Write w = ackMsg.write;
      if (isPrimary() && ackMsg.write.csn == Integer.MAX_VALUE) {
        w.csn = getCSN();
        newCommitted = true;
      }
      writeLog.add(w);
      refreshPlayList();

      vv.put(rid.toString(), currTimeStamp());
      vv.put(w.replicaId.toString(), w.acceptTime);

      // update maxCSN;
      if (!isPrimary() && w.csn != Integer.MAX_VALUE) {
        maxCSN = Math.max(maxCSN, w.csn);
      }

      if (newCommitted) {
        doGossip();
      } else {
        doGossip(ackMsg.src);
      }
    }
  }


  /**
   * Refresh playlist on every new committed write,
   * as there might be some potential roll back.
   */
  public void refreshPlayList() {
    Hashtable<String, String> tmp = new Hashtable<String, String> ();
    Iterator<Write> it = writeLog.getIterator();
    while (it.hasNext()) {
      Command cmd = it.next().command;
      if (cmd instanceof ClientCmd) {
        if (cmd instanceof Put) {
          Put put = (Put) cmd;
          tmp.put(put.song, put.url);
        } else if (cmd instanceof Del) {
          Del del = (Del) cmd;
          tmp.remove(del.song);
        } else if (cmd instanceof Get) {
          // no change here
        }
      } else if (cmd instanceof ServerCmd) {
        ServerCmd scmd = (ServerCmd) cmd;
        if (scmd instanceof Create) {
          String id = scmd.rid.toString();
          if (!vv.hasKey(id)) {
            vv.put(id, scmd.acceptTime);
          }
        } else if (scmd instanceof Retire) {
          String id = scmd.rid.toString();
          if (vv.hasKey(id)) {
            vv.remove(id);
          }
          if (connected.contains(scmd.rid.pid)) {
            connected.remove(((Integer)scmd.rid.pid));
          }
        }
      }
    }
    playList.pl = tmp;
  }


  public void connectTo(int id) {
   if (!connected.contains(id)) {
     connected.add(id);
     send(new AERqstMsg(pid, id));
     if (debug) {
       print("Reconnected with " + id);
       print(connected.toString());
     }
   }
  }

  public void disconnectWith(int id) {
    if (connected.contains(id)) {
      connected.remove((Integer)id);
      if (debug) {
        print("Disconnected with " + id);
        print(connected.toString());
      }
    } else {
      print("Disconnected with " + id + " no connection found...");
      print(connected.toString());
    }
  }

  public void updateConnected(ArrayList<Integer> serverList) {
    connected = new ArrayList<Integer>(serverList);
    if (connected.contains(pid)) {
      connected.remove((Integer) pid);
    }
    if (debug) {
      print("New Server joined: ");
      print(connected.toString());
    }
  }

  public void printLog() {
    writeLog.print();
  }


  /**
   * Receive retire command from master
   * Issue retire write to itself
   * must be called by Master thread for blocking.
   */
  public synchronized void toRetire() {
    int acceptTime = nextTimeStamp();
    int csn = isPrimary() ? getCSN() : Integer.MAX_VALUE;
    Retire rcmd = new Retire(rid, acceptTime);
    Write entry = new Write(csn, acceptTime, rid, rcmd);
    writeLog.add(entry);
    vv.put(rid.toString(), currTimeStamp());

    /* assume has at least one neighbor right now (piazza @86)
     * otherwise need keep gossiping periodically.
     */
    assert connected.size() != 0;
    doGossip();
    toRetire = true;
    while (!retired) {
      try {
        wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Finishing the retirement protocol
   *  if primary, hand off duty
   *  stop looping
   *  unblock master thread
   *
   *  release all resources
   */
  public synchronized void retire(int src) {
    if (isPrimary) {
      // set up new primary with current CNS counter, globally unique
      send(new PrimaryHandOffMsg(pid, src, csnStamp));
    }
    retired =true;
    shutdown = true;
    nc.shutdown();
    // wake up the blocked master
    notifyAll();
  }
}

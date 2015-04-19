package exec;

import command.*;
import msg.*;
import util.PlayList;
import util.ReplicaID;
import util.Write;
import util.WriteLog;

import java.util.*;

/**
 * Server/Replica class
 */
public class Server extends NetNode {
  int primaryId;

  int timeStamp;
  int csnStamp;

  int maxCSN;

  ReplicaID rid;
  ArrayList<Integer> connected;
  PlayList playList;
  WriteLog writeLog;

  Map<String, Integer> versionVector;

  public Server(int id) {
    super(id);
    rid = new ReplicaID(id);
    primaryId = 0;
    initialization();
    start();
  }

  public Server(int id, int primary) {
    super(id);
    primaryId = primary;
    initialization();
    send(new CreateMsg(pid, primaryId));
    start();
  }

  public void initialization() {
    timeStamp = 0;
    csnStamp = 0;
    maxCSN = 0;

    connected = new ArrayList<Integer>();
    writeLog = new WriteLog();
    playList = new PlayList();

    versionVector = new HashMap<String, Integer>();
  }


  public String name () {
    return "Server";
  }

  public boolean isPrimary() {
    return pid == primaryId;
  }

  public int nextTimeStamp () {
    return ++timeStamp;
  }

  public int currTimeStamp () {
    return timeStamp;
  }

  public int getCSN() {
    maxCSN = csnStamp;
    return csnStamp++;
  }

  /**
   * Main Server Thread, message handler
   */
  public void run() {
    if (debug) {
      print("Started");
    }
    while (true){
      Message msg = receive();
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
        send(new AERplyMsg(pid, msg.src, versionVector, maxCSN));
      } else if (msg instanceof AERplyMsg) {
        AERplyMsg m = (AERplyMsg) msg;
        anti_entropy(m);
      } else if (msg instanceof AEAckMsg) {
        AEAckMsg ackMsg = (AEAckMsg) msg;
        antiEntropyACKHandler(ackMsg);
      }
    }
  }


  /**
   * Initialization upon receiving reply from create write
   * @param cReply
   */
  public void setUpServer(CreateReplyMsg cReply) {
    rid = cReply.rid;
    timeStamp = rid.acceptTime + 1;
    versionVector.put(rid.toString(), currTimeStamp());
  }


  /**
   *  initiate anti-entropy process with every connected node
   *
   */
  public void doGossip() {
    for (int node : connected) {
      send(new AERqstMsg(pid, node));
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
    if (!rqst.sm.isDominatedBy(versionVector)) {
      send(new ClientReplyMsg(rqst));
      return;
    }
    if (rqst.isWrite()) {
      if (playList.update(rqst.cmd)) {
        int acceptTime = nextTimeStamp();
        int csn = isPrimary() ? getCSN() : Integer.MAX_VALUE;
        Write write = new Write(csn, acceptTime, rid, rqst.cmd);
        writeLog.add(write);
        versionVector.put(rid.toString(), currTimeStamp());
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
    Create create = new Create(rid, acceptTime);
    Write entry = new Write(csn, acceptTime, rid, create);
    writeLog.add(entry);

    versionVector.put(rid.toString(), currTimeStamp());
    versionVector.put(newId.toString(), 0);

    // send ack to server
    send(new CreateReplyMsg(pid, msg.src, newId));
    // update with neighbors, anti-entropy
    doGossip();
  }


  /**
   * Propagating committed writes upon receiving reply from R
   * Complex logic, can be buggy
   */
  public void anti_entropy(AERplyMsg msg) {
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
      Write w = ackMsg.write;
      if (isPrimary() && ackMsg.write.csn == Integer.MAX_VALUE) {
        w.csn = getCSN();
      }
      writeLog.add(w);
      refreshPlayList();
      versionVector.put(rid.toString(), currTimeStamp());
      doGossip();
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
        if (cmd instanceof Create) {
          String id = scmd.rid.toString();
          if (!versionVector.containsKey(id)) {
            versionVector.put(id, 0);
          }
        } else if (cmd instanceof Retire) {
          String id = scmd.rid.toString();
          if (versionVector.containsKey(id)) {
            versionVector.remove(id);
          }
          if (connected.contains(scmd.rid.pid)) {
            connected.remove(scmd.rid.pid);
          }
        }
      }
    }
    playList.pl = tmp;
  }

  public void connectTo(List<Integer> upList) {
    for (int i : upList) {
      connected.add(i);
    }
  }

  public void updateConnected(ArrayList<Integer> serverList) {
    connected = new ArrayList<Integer>(serverList);
    if (connected.contains(pid)) {
      connected.remove((Integer) pid);
    }
    print(connected.toString());
  }

  public void printLog() {
    writeLog.print();
  }
}

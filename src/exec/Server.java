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
  Set<Integer> connected;
  PlayList playList;
  WriteLog writeLog;

  Map<String, Integer> versionVector;

  public Server(int id) {
    super(id);
    primaryId = 0;
    timeStamp = 0;
    csnStamp = 0;
    maxCSN = 0;

    connected = new HashSet<Integer>();
    writeLog = new WriteLog();
    playList = new PlayList();

    versionVector = new HashMap<String, Integer>();


  }

  public void run() {
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
        send(new AERplyMsg(pid, msg.dst, versionVector, maxCSN));
      } else if (msg instanceof AERplyMsg) {
        AERplyMsg m = (AERplyMsg) msg;
        anti_entropy(m);
      } else if (msg instanceof AEAckMsg) {
        AEAckMsg ackMsg = (AEAckMsg) msg;
        ackHandler(ackMsg);
      }

    }
  }

  /**
   * handler for write updates through anti-entropy process
   * @param ackMsg
   */
  public void ackHandler(AEAckMsg ackMsg) {
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
      // todo
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
   *
   */
  public void originalClientCmd (ClientMsg rqst) {
    int acceptTime = nextTimeStamp();
    int csn = isPrimary() ? getCSN() : Integer.MAX_VALUE;
    Write entry = new Write(csn, acceptTime, rid, rqst.cmd);
    writeLog.add(entry);
    boolean suc = playList.update(rqst.cmd);
    versionVector.put(rid.toString(), currTimeStamp());

    // send ack to client


    // update with neighbors
    doGossip();
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
    Write entry = new Write(csn, acceptTime, rid, msg.cmd);
    writeLog.add(entry);

    versionVector.put(rid.toString(), currTimeStamp());
    versionVector.put(newId.toString(), 0);

    // send ack to server
    CreateReplyMsg crMsg = new CreateReplyMsg(pid, msg.src, newId);
    send(crMsg);

    // update with neighbors, anti-entropy
    doGossip();
  }


  /**
   * Propagating committed writes upon receiving reply from R
   *
   */
  public void anti_entropy(AERplyMsg msg) {
    List<Write> unknownWrites = new ArrayList<Write> ();
    Iterator<Write> it = writeLog.getIterator();
    while (it.hasNext()) {
      Write w = it.next();
      String wKey = w.replicaId.toString();
      if (w.csn <= msg.CNS) {
        /* already committed in R */
        continue;
      }  else if (w.csn < Integer.MAX_VALUE) {
        /*  committed write unknown to R */
        if (msg.versionVector.containsKey(wKey) &&
            w.acceptTime <= msg.versionVector.get(wKey)) {
            /* R has the write, but doesn't know it is committed  */
            send(new AEAckMsg(pid, msg.src, w, true));
        } else {
            send(new AEAckMsg(pid, msg.src, w));
        }
      } else {
        /* all tentative writes */
        int rvTime = msg.versionVector.containsKey(wKey) ?
                      msg.versionVector.get(wKey) : -1;
        if (rvTime < w.acceptTime) {
          send(new AEAckMsg(pid, msg.src, w));
        }
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
}

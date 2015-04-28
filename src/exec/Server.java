package exec;

import command.*;
import msg.*;
import util.PlayList;
import util.ReplicaID;
import util.Write;
import util.WriteLog;
import util.UndoLog;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Server/Replica class
 */
public class Server extends NetNode {
  int timeStamp = 0;
  int csnStamp = 0;
  boolean isPrimary = false;

  int maxCSN = 0;

  ReplicaID rid = null;
  ArrayList<Integer> connected = new ArrayList<Integer>();
  PlayList playList = new PlayList();
  WriteLog writeLog = new WriteLog();
  UndoLog undoLog = new UndoLog();

  Map<String, Integer> versionVector = new HashMap<String, Integer>();

  boolean toRetire = false;
  boolean retired = false;
  boolean started = false; // YW: boolean of whether the server has registered at other server, has a Replica id and accept-stamp
  public Lock retireLock = new ReentrantLock();
  /**
   * Primary server, start with id = 0 //YW not with id 0
   * @param id
   */
  public Server(int id) {
    super(id);
    rid = new ReplicaID(id);
    isPrimary = true;
    started = true;
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
    return ++timeStamp;
  }

  public int currTimeStamp () {
    return timeStamp;
  }

  public int getCSN() { // YW: only called when committing a new write as primary server
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
    while (!retired){
      Message msg = receive();
      /*YW: if not started, only accept CreateReplyMsg*/
      if(!started && !(msg instanceof CreateReplyMsg)){
    	  if(debug){
    		  System.out.println("YW: Message received before started" + msg.toString());
    	  }
    	  continue;
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
      }
      /*receive create reply msg to start up server*/
      else if (msg instanceof CreateReplyMsg) {
        CreateReplyMsg cReply = (CreateReplyMsg) msg;
        setUpServer(cReply);
      }
      /* anti-entropy request message */
      else if (msg instanceof AERqstMsg) {
        send(new AERplyMsg(pid, msg.src, versionVector, maxCSN));
      }
      else if (msg instanceof AERplyMsg) {
        AERplyMsg m = (AERplyMsg) msg;
        anti_entropy(m); // YW: handle reply and send writes to receiver
        if (toRetire) {
          retire(msg.src);
        }
      } 
      else if (msg instanceof AEAckMsg) {
        AEAckMsg ackMsg = (AEAckMsg) msg;
        antiEntropyACKHandler(ackMsg);
      }
      // Handling AEMultiAckMsg
      else if(msg instanceof AEMultiAckMsg){
    	  AEMultiAckMsg multiAckMsg = (AEMultiAckMsg)msg;
    	  antiEntropyMultiACKHandler(multiAckMsg); //YW: Receive and update writelog
      }

      /* primary handoff Message */
      else if (msg instanceof PrimaryHandOffMsg) {
        PrimaryHandOffMsg pho = (PrimaryHandOffMsg) msg;
        isPrimary = true;
        csnStamp = pho.curCSN;
        //YW: Change all tentative writes to committed writes
        maxCSN = csnStamp - 1;
        startPrimary();
      }
    }
  }


  /**
   * Initialization upon receiving reply from create write
   * @param cReply
   */
  public void setUpServer(CreateReplyMsg cReply) {
    rid = cReply.rid;
    //timeStamp = rid.acceptTime + 1;
    timeStamp = rid.acceptTime; // YW: TODO: Don't need plus 1 here, since we are always assigning the next timestamp to writes
    versionVector.put(rid.toString(), currTimeStamp());
    started = true;
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
    if (!rqst.sm.isDominatedBy(versionVector)) {
      send(new ClientReplyMsg(rqst));
      return;
    }
    if (rqst.isWrite()) {
    	//YW: change how playlist is updated to help update undo list
    	if(playList.checkFeasibility(rqst.cmd)){
    		int acceptTime = nextTimeStamp();
    		int csn = isPrimary()?getCSN():Integer.MAX_VALUE;
    		Write write = new Write(csn,acceptTime, rid, rqst.cmd);
    		undo(write);
    		writeLog.add(write);
    		updatePlayList(write);
    		send(new ClientReplyMsg(rqst,write));
    		doGossip();
    	}else{
    		//YW: Drop the write TODO: Is this necessary?
            send(new ClientReplyMsg(rqst));
    	}
    		
    	/*	
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
      }*/
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
    
    undo(entry);
    writeLog.add(entry);
    updatePlayList(entry); // Update UndoLog for this write
    
    /*version update is done in updatePlayList*/
    /*
    versionVector.put(rid.toString(), currTimeStamp());
    //versionVector.put(newId.toString(), 0);//YW: what about creating version with acceptTime ?
    versionVector.put(newId.toString(), acceptTime); // TODO: Check whether this is necessary
    */
    
    // send ack to server
    send(new CreateReplyMsg(pid, msg.src, newId));
    // update with neighbors, anti-entropy
    /*
     * YW: in current setting, connection is updated after the new server started
     * here server connect with all existing servers, while the new server do 
     * AE with all servers.
     */
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
        }
        /* YW: this else condition is not necessary, sender just send 
        all the writes to receiver and let receiver decide*/
        else { 
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
   * YW: handler for anti-entropy reply at Sender, sends AEMultiAckMsg to receiver to update receiver's writelog
   * @param aeReplyMsg
   */
  public void antiEntropyReplyHandler(AERplyMsg replyMsg){
	  Iterator<Write> iter = writeLog.getIterator();
	  AEMultiAckMsg aeMultiAckMsg = new AEMultiAckMsg(pid, replyMsg.src);
	  while(iter.hasNext()){
		  Write wr = iter.next();
		  String ownerServer = wr.replicaId.toString();
		  String parentServer = wr.replicaId.parent.toString(); // Parent of the 
		  if(wr.csn <= replyMsg.CNS){// If the write is already committed in the receiver, ignore this write
			  continue;
		  }
		  else{
			  Integer completeVersion = completeV(replyMsg.versionVector,wr.replicaId);
			  if (wr.acceptTime <= completeVersion){ // If the receiver has the write but doesn't know the write is committed
				  aeMultiAckMsg.addMsg(new AEAckMsg(pid, replyMsg.src, wr, true));
			  }else{ // The receiver does not know the write
				  aeMultiAckMsg.addMsg(new AEAckMsg(pid,replyMsg.src,wr));
			  }
		  }
	  }
	  send(aeMultiAckMsg);
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
      versionVector.put(rid.toString(), currTimeStamp());
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
   * YW: handler for packed multiple ACKs
   */
  
  public void antiEntropyMultiACKHandler(AEMultiAckMsg multiAckMsg){
	  if(multiAckMsg.msgList.isEmpty()){// if no updates acquired, do nothing
		  return;
	  }
	  AEAckMsg firstAckMsg = (AEAckMsg) multiAckMsg.msgList.get(0);
	  Write firstWrite = firstAckMsg.write; // The first change in writelog
	  boolean newCommitted = false;
	  //1. Accept all writes
	  for(Message msg : multiAckMsg.msgList){
		  if(msg instanceof AEAckMsg){
			  AEAckMsg tempMsg = (AEAckMsg) msg;
			  boolean writeNotReceived = updateVersionVector(tempMsg.write);
			  
			  if(tempMsg.commit){
				  if (writeLog.commit(tempMsg.write)) {
					/* successfully updated */
					  maxCSN = Math.max(maxCSN, tempMsg.write.csn);
			      }else{
			    	  //write not found in writelog
			      }
			  }else{
			      Write w = tempMsg.write;
				  if(!writeNotReceived){ // The write is already received before
				      if (w.csn == Integer.MAX_VALUE) {// ignore already received tentative write
				    	  continue;
				      }else{
				    	  if(writeLog.commit(w)){
				    		  maxCSN = Math.max(maxCSN, w.csn);
				    	  }
				      }
				  }
				  else{// Write not received before
					  if (isPrimary() && tempMsg.write.csn == Integer.MAX_VALUE) {
						  w.csn = getCSN();
						  newCommitted = true;
					  }
					  writeLog.add(w);
				      if (!isPrimary() && w.csn != Integer.MAX_VALUE) {
				    	  maxCSN = Math.max(maxCSN, w.csn);
				      }
			      }
			  }
		  }else{
			  //Something wrong with the generation of AEMultiAckMsg
		  }
	  }
	  //2. Undo till the first Write
      undo(firstWrite);
	  //3. Update playlist and accordingly the undo list
	  updatePlayList(firstWrite);
	  
      if (newCommitted) {
        doGossip();
      } else {
        doGossip(multiAckMsg.src);
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
  public void toRetire() {
    int acceptTime = nextTimeStamp();
    int csn = isPrimary() ? getCSN() : Integer.MAX_VALUE;
    Retire rcmd = new Retire(rid, acceptTime);
    Write entry = new Write(csn, acceptTime, rid, rcmd);
    writeLog.add(entry);
    versionVector.put(rid.toString(), currTimeStamp());

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
   */
  public void retire(int src) {
    if (isPrimary) {
      // set up new primary with current CNS counter, globally unique
      send(new PrimaryHandOffMsg(pid, src, csnStamp));
    }
    retired =true;
    // wake up the blocked master
    notifyAll();
  }
  
  /**
   * YW: Complete version vector (deciding whether the server is known retired to a version vector)
   * @param Version Vector, ReplicaID
   */
  public Integer completeV(Map<String, Integer> vv,ReplicaID S){
	  if(vv.containsKey(S.toString())){
		  return vv.get(S);
	  }else if(S.parent == null){
		  return Integer.MAX_VALUE;
	  }else{
		  ReplicaID parent_rid = S.parent;
		  if(completeV(vv, S.parent) >= S.acceptTime){
			  return Integer.MAX_VALUE;
		  }else{
			  return Integer.MIN_VALUE;
		  }
	  }
  }
  /**
   * YW: Helper function to update version vector based on one single write, return true if write accepted, return false if not
   */
  public boolean updateVersionVector(Write wr){

	  ReplicaID writeRid = wr.replicaId;
	  Integer completeVersion = completeV(versionVector, writeRid); // return the complete version of the Write's RID
	  boolean ans;
	  
	  if(completeVersion < wr.acceptTime){//Write not seen before, update version vector
		  if(wr.command instanceof ServerCmd){ // The creation/Retire information is not received before
			  ServerCmd serverCmd = (ServerCmd) wr.command;
			  if(serverCmd instanceof Create){
				  versionVector.put(serverCmd.rid.toString(), serverCmd.acceptTime);
			  }else if(serverCmd instanceof Retire){
				  versionVector.remove(serverCmd.rid.toString());
			  }
		  }
		  versionVector.put(wr.replicaId.toString(), wr.acceptTime);
		  ans = true;
	  }else{ // Write already received, no need to update
		  ans = false;
	  }  
	  return ans;
  }
  
  /**
	 * Update the playlist according to writelog, and update undo log as well (Combine undo and update to replace refreshPlayList)
	 * version vector is already updated when the write is received
	 * @param firstChange
	 */
	public void updatePlayList(Write firstChange){
		Iterator<Write> it = writeLog.getIterator();
		while(it.hasNext()){
			Write tempWrite = it.next();
			Command cmd = tempWrite.command;
			if(tempWrite.compareTo(firstChange)>=0){ // Compare to the first change
				Write copyWrite = getUndoEntry(tempWrite);
				if(copyWrite.csn < Integer.MAX_VALUE){
					// committed, don't need undo log, but need to update playList and maxCSN
					//maxCSN = Math.max(copyWrite.csn, maxCSN); //YW: Already contained in AEMultiAckHandler
				}
				else{ // for tentative write, first push generated undoentry to undolog, then update playlist
					undoLog.push(copyWrite);
				}
				if(cmd instanceof ClientCmd){
					playList.update((ClientCmd) cmd);
				}
			}else{
				// write is before the first changed write, ignore
			}
			if(cmd instanceof Retire){
				Retire retireCmd = (Retire) cmd;
				if (connected.contains(retireCmd.rid.pid)) {
					connected.remove(retireCmd.rid.pid);
				}
			}
		}
		
	}//updata undo log for corresponding write
	/**
	 * Generate undo log for each write
	 * @return
	 */
	public Write getUndoEntry(Write wr){
		Write copyWrite = new Write(wr);
		Command cmd = wr.command;
		if(cmd instanceof ClientCmd){ // if command is client command and not committed
			if(cmd instanceof Put){
				Put put = (Put) cmd;
				if(playList.containsSong(put.song)){ // There is song same in the playlist
					copyWrite.command = new Put(put.song,playList.get(put.song));
				}else{// The song is new to playlist
					copyWrite.command = new Del(put.song);
				}
			}else if(cmd instanceof Del){
				Del del = (Del) cmd;
				if(playList.containsSong(del.song)){ // If the song exists, put song back
					copyWrite.command = new Put(del.song,playList.get(del.song));
				}else{ // Song doesn't exist, no need to change undo list
				}
			}else{// cmd instance of Get
			}
		}else{ // not ClientCmd, just push original write to Undo Log
		}
		return copyWrite;
	}
	
	/**
	 * YW: Undo the playlist until reaching the earlist write
	 */
	public void undo(Write wr){
		while(!undoLog.isEmpty() && undoLog.lastEntry().compareTo(wr) >= 0){
			Command cmd = undoLog.pop().command;
			if(cmd instanceof ClientCmd){
				playList.update((ClientCmd)cmd);
			}
		}
	}
	
	/**
	 * YW: Called when become primary server, commit all tentative writes in writeLog
	 */
	public void startPrimary(){
		Iterator<Write> it = writeLog.getIterator();
		while(it.hasNext()){
			Write tempWrite = it.next();
			if(tempWrite.csn < Integer.MAX_VALUE){
				continue;
			}
			else{
				tempWrite.csn = this.getCSN(); // commit tentative writes
			}
		}
	}
  
  
}

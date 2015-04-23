package exec;

import command.*;
import msg.*;
import util.SessionManager;

/**
 * Client code
 *  1. send request to server, controlled by Master
 *  2. Receive server's reply, update session manager
 *
 */

public class Client extends NetNode {
  int serverId;
  SessionManager sm;

  public Client (int pid, int sid) {
    super(pid);
    serverId = sid;
    sm = new SessionManager();
    start();
  }

  public String name () {
    return "Client";
  }

  /**
   * Handle received message
   */
  public void run() {
    if (debug) {
      print("Started");
    }
    while (true){
      Message msg = receive();
      if (msg instanceof ClientReplyMsg) {
        ClientReplyMsg rqstRply = (ClientReplyMsg) msg;
        if (rqstRply.cmd instanceof Put) {
          if (rqstRply.suc) {
            sm.updateWrite(rqstRply.write);
          } else {
            // todo
            System.out.print("Put failed, to be updated");
          }
        }
        if (rqstRply.cmd instanceof Del) {
          if (rqstRply.suc) {
            sm.updateWrite(rqstRply.write);
          } else {
            // todo
            System.out.print("Del failed, to be updated");
          }
        }
        if (rqstRply.cmd instanceof Get) {
          if (rqstRply.suc) {
            sm.updateRead(rqstRply.write);
            System.out.println(rqstRply.cmd.song + ":" + rqstRply.url);
          } else {
            System.out.println(rqstRply.cmd.song + ":ERR_DEP");
          }
        }
      }
    }
  }

  /**
   * Put command, Write
   * @param name key
   * @param url  value
   */
  public void put (String name, String url) {
    ClientCmd cmd = new Put(name, url);
    ClientMsg rqst = new ClientMsg(pid, serverId, cmd, sm);
    send(rqst);
  }

  /**
   * Delete command, Write
   * @param name key
   */
  public void del (String name) {
    ClientCmd cmd = new Del(name);
    ClientMsg rqst = new ClientMsg (pid, serverId, cmd, sm);
    send(rqst);
  }

  /**
   * Get command, Read
   * @param name key
   */
  public void get (String name) {
    ClientCmd cmd = new Get(name);
    ClientMsg rqst = new ClientMsg (pid, serverId, cmd, sm);
    send(rqst);
  }
  /*
   * YW: disconnect with server
   * @param disconnectedServerId
   */
  public void disconnectWith (int disconnectServerId){
	  if(disconnectServerId == this.serverId){ //the node is currently connected to disconnectServerId
		  serverId = -1; // Indication of not currently connected
	  }else{
		  //Do nothing or display no server connected
	  }
  }
  /*
   * YW: connect with server
   * @param connectedServerId
   */
  public void connectTo (int connectServerId){
	  if(serverId == -1){
		  serverId = connectServerId;
	  }else if(serverId == connectServerId){
		  // Do nothing, already connected to current server
	  }else{
		  //Display error message, connecting to multiple servers
	  }
  }
  
}




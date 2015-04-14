package exec;

import command.Command;
import command.Del;
import command.Put;
import msg.ClientRequest;
import msg.Message;
import msg.RequestReply;

/**
 * Created by xiaofan on 3/28/15.
 */

public class Client extends NetNode {
  int serverId;

  public Client (int pid, int sid, int numServers, int numClients) {
    super(pid);
    serverId = sid;
  }

  public void run() {
    while (true){
      Message msg = receive();
      if (msg instanceof RequestReply) {
        RequestReply rqstRply = (RequestReply) msg;
        if (rqstRply.command instanceof Put) {

        }
        if (rqstRply.command instanceof Del) {

        }
      }
    }
  }

  public void put (String name, String url) {
    Command cmd = new Put(name, url);
    ClientRequest rqst = new ClientRequest(pid, serverId, cmd);
    send(rqst);
  }

  public void del (String name) {
    Command cmd = new Del(name);
    ClientRequest rqst = new ClientRequest(pid, serverId, cmd);
    send(rqst);
  }
}




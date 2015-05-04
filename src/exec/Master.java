package exec;

import util.Constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class Master {

  public static void main(String [] args) {
    NetNode[] nodes = new NetNode[Constants.MAX_NODE];
    ArrayList<Integer> curServers = new ArrayList<Integer>();


    Scanner scan = new Scanner(System.in);
    while (scan.hasNextLine()) {
      String [] inputLine = scan.nextLine().split(" ");
      int clientId, serverId, id1, id2;
      String songName, URL;
      if (Constants.debug) {
        System.out.println("Command: " + Arrays.toString(inputLine));
      }


      if (inputLine[0].equals("joinServer")) {
        serverId = Integer.parseInt(inputLine[1]);
        /**
         * Start up a new server with this id and connect it to all servers
         */
        if (curServers.isEmpty()) {
          nodes[serverId] = new Server(serverId);
        } else {
          nodes[serverId] = new Server(serverId, curServers.get(0));
        }
        takeSnap(Constants.SLEEP); //YW: wait till the server is started
        curServers.add(serverId);
        //YW: the new server should connect to all existing servers
        //but old servers should not connect with each other
        Server s = (Server) nodes[serverId];
        s.updateConnected(curServers);
        for(int i : curServers){
        	assert nodes[i] != null;
        	assert nodes[i] instanceof Server;
        	if(i != serverId){
        		((Server) nodes[i]).connectTo(serverId);
        	}
        }
        /*
        for (int i: curServers) {
          assert nodes[serverId] != null;
          assert nodes[serverId] instanceof Server;
          Server s = (Server)nodes[i];
          s.updateConnected(curServers);
        }*/
        takeSnap(Constants.SLEEP);

      } else if (inputLine[0].equals("retireServer")) {
    	  takeSnap(Constants.SLEEP); //YW: wait for client messages
        serverId = Integer.parseInt(inputLine[1]);
	      /**
	       * Retire the server with the id specified. This should block until
	       * the server can tell another server of its retirement
         */
        assert nodes[serverId] != null;
        assert nodes[serverId] instanceof Server;
        Server s = (Server)nodes[serverId];
        s.toRetire();
        //YW:
        curServers.remove(serverId);
        for(int i : curServers){
        	if(nodes[i] instanceof Server){
        		Server temp_node = (Server)nodes[i];
        		temp_node.disconnectWith(serverId);
        	}// Other servers disconnects with servers
        	//Clients should disconnect with server based on how retire works
        }
        //Disconnect all links between serverId and servers / clients

      } else if (inputLine[0].equals("joinClient")) {
        clientId = Integer.parseInt(inputLine[1]);
        serverId = Integer.parseInt(inputLine[2]);
         /**
          * Start a new client with the id specified and connect it to
	        * the server
          */
        nodes[clientId] = new Client(clientId, serverId);
      } else if (inputLine[0].equals("breakConnection")) {
        id1 = Integer.parseInt(inputLine[1]);
        id2 = Integer.parseInt(inputLine[2]);
         /**
          * Break the connection between a client and a server or between
	        * two servers
          */
        assert nodes[id1] != null;
        assert nodes[id2] != null;
        if (nodes[id1] instanceof Server && nodes[id2] instanceof Server) {
          Server s1 = (Server)nodes[id1];
          s1.disconnectWith(id2);
          Server s2 = (Server)nodes[id2];
          s2.disconnectWith(id1);
        } else if (nodes[id1] instanceof Client && nodes[id2] instanceof Server) {
        	//YW
        	Client c = (Client)nodes[id1];
        	c.disconnectWith(id2);
        } else if (nodes[id2] instanceof Client && nodes[id1] instanceof Server) {
        	//YW
        	Client c = (Client)nodes[id2];
        	c.disconnectWith(id1);
        } else {
        	System.out.println("Invalid Operation, disconnecting two clients");
        }
      } else if (inputLine[0].equals("restoreConnection")) {
        id1 = Integer.parseInt(inputLine[1]);
        id2 = Integer.parseInt(inputLine[2]);
         /**
          * Restore the connection between a client and a server or between
	        * two servers
          */
        assert nodes[id1] != null;
        assert nodes[id2] != null;
        if (nodes[id1] instanceof Server && nodes[id2] instanceof Server) {
          Server s1 = (Server)nodes[id1];
          s1.connectTo(id2);
          Server s2 = (Server)nodes[id2];
          s2.connectTo(id1);
        } else if (nodes[id1] instanceof Client && nodes[id2] instanceof Server) {
        	//YW
        	Client c = (Client)nodes[id1];
        	c.connectTo(id2);
        } else if (nodes[id2] instanceof Client && nodes[id1] instanceof Server) {
        	//YW
        	Client c = (Client)nodes[id2];
        	c.connectTo(id1);
        } else {
        	System.out.println("Invalid Operation, disconnecting two clients");
        }

      } else if (inputLine[0].equals("pause")) {
         /**
          * Pause the system and don't allow any Anti-Entropy messages to
	        * propagate through the system
          */
    	  /*
    	   * YW
    	   */
    	  for (int server : curServers){
    		  if(nodes[server] != null){
    			  nodes[server].pause();
    		  }
    	  }

      } else if (inputLine[0].equals("start")) {
         /**
          * Resume the system and allow any Anti-Entropy messages to
	        * propagate through the system
	        */
    	  for(int id: curServers){
    		  nodes[id].unPause();
    	  }

      } else if (inputLine[0].equals("stabilize")) {
          /**
           *  Block until there are enough Anti-Entropy messages for all values to
           * propagate through the currently connected servers. In general, the
           * time that this function blocks for should increase linearly with the
	         * number of servers in the system.
	         */
        takeSnap(curServers.size() * Constants.SLEEP);

      } else if (inputLine[0].equals("printLog")) {
        serverId = Integer.parseInt(inputLine[1]);
         /**
          * Print out a server's operation log in the format specified in the
	        * handout.
	        */
        assert nodes[serverId] != null;
        assert nodes[serverId] instanceof Server;
        Server s = (Server)nodes[serverId];
        s.printLog();

      } else if (inputLine[0].equals("put")) {
        clientId = Integer.parseInt(inputLine[1]);
        songName = inputLine[2];
        URL = inputLine[3];
         /**
          * Instruct the client specified to associate the given URL with the given
	        * songName. This command should block until the client communicates
          * with one server.
          */
        assert nodes[clientId] != null;
        assert nodes[clientId] instanceof Client;
        Client c = (Client)nodes[clientId];
        c.put(songName, URL);
       // takeSnap(Constants.SLEEP);
      } else if (inputLine[0].equals("get")) {
        clientId = Integer.parseInt(inputLine[1]);
        songName = inputLine[2];
         /**
          * Instruct the client specified to attempt to get the URL associated with
	        * the given songName. The value should then be printed to standard
	        * out of the master script in the format specified in the handout.
	        * This command should block until the client communicates with one server.
	        */
        assert nodes[clientId] != null;
        assert nodes[clientId] instanceof Client;
        Client c = (Client)nodes[clientId];
        c.get(songName);
        takeSnap(Constants.SLEEP);
      } else if (inputLine[0].equals("delete")) {
        clientId = Integer.parseInt(inputLine[1]);
        songName = inputLine[2];
         /**
          * Instruct the client to delete the given songName from the playlist.
          * This command should block until the client communicates with one server.
          */
        assert nodes[clientId] != null;
        assert nodes[clientId] instanceof Client;
        Client c = (Client)nodes[clientId];
        c.del(songName);
        //takeSnap(Constants.SLEEP);
      }
    }
    // all done
    takeSnap(500);
    System.exit(0);
  }

  public static void takeSnap(int time) {
    try {
      Thread.sleep(time);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}

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
        curServers.add(serverId);
        for (int i: curServers) {
          assert nodes[serverId] != null;
          assert nodes[serverId] instanceof Server;
          Server s = (Server)nodes[i];
          s.updateConnected(curServers);
        }



      } else if (inputLine[0].equals("retireServer")) {
        serverId = Integer.parseInt(inputLine[1]);
	      /**
	       * Retire the server with the id specified. This should block until
	       * the server can tell another server of its retirement
         */

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

      } else if (inputLine[0].equals("restoreConnection")) {
        id1 = Integer.parseInt(inputLine[1]);
        id2 = Integer.parseInt(inputLine[2]);
         /**
          * Restore the connection between a client and a server or between
	        * two servers
          */

      } else if (inputLine[0].equals("pause")) {
         /**
          * Pause the system and don't allow any Anti-Entropy messages to
	        * propagate through the system
          */

      } else if (inputLine[0].equals("start")) {
         /**
          * Resume the system and allow any Anti-Entropy messages to
	        * propagate through the system
	        */

      } else if (inputLine[0].equals("stabilize")) {
          /**
           *  Block until there are enough Anti-Entropy messages for all values to
           * propagate through the currently connected servers. In general, the
           * time that this function blocks for should increase linearly with the
	         * number of servers in the system.
	         */

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
      }
    }
  }
}

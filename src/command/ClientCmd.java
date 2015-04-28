package command;

/**
 * Created by xiaofan on 4/13/15.
 */
public class ClientCmd extends Command {
  public String song;
  public String url;

  public ClientCmd(ClientCmd cmd){
	  song = cmd.song;
	  url = cmd.url;
  }
  public ClientCmd (String s) {
    song = s;
    url = "";
  }

  public ClientCmd(String s, String u) {
    song = s;
    url = u;
  }
}

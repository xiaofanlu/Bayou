package util;

import command.ClientCmd;
import command.Del;
import command.Put;

import java.util.Hashtable;
import java.util.Map;


public class PlayList {
  public Hashtable<String, String> pl;

  public PlayList () {
    pl = new Hashtable<String, String>();
  }

  public boolean containsSong(String song) {
    return pl.containsKey(song);
  }
  
  public boolean add(String song, String url){
    pl.put(song, url);
    return true;
  }

  public boolean delete(String song){
    if(pl.containsKey(song)){
      pl.remove(song);
      return true;
    } else {
      return false;
    }
  }

  public String get(String song) {
    if (pl.containsKey(song)) {
      return pl.get(song);
    } else {
      return "NOT_FOUND";
    }
  }

  public boolean update (ClientCmd cmd) {
    if (cmd instanceof Put) {
      return add(cmd.song, cmd.url);
    } else if (cmd instanceof Del) {
      return delete(cmd.song);
    } else {
      return pl.containsKey(cmd.song);
    }
  }

  public void print() {
    System.out.println();
    System.out.println("++++++++++++++PlayList++++++++++++");
    int i = 1;
    for (Map.Entry<String, String> item : pl.entrySet()) {
      System.out.println(i + ". Song Name: " + item.getKey() + "\tURL: " + item.getValue());
      i++;
    }
    System.out.println("+++++++++++++++ End  +++++++++++++");
    System.out.println();
  }


}

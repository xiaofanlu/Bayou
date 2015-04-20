package msg;

/**
 * Created by xiaofan on 4/20/15.
 */
public class PrimaryHandOffMsg extends Message {
  public int curCSN;
  public PrimaryHandOffMsg(int s, int d, int CSN) {
    super(s, d);
    curCSN = CSN;
  }

  public String toString() {
    return super.toString() + "HandOff(" + curCSN + ")";
  }
}

package msg;

/**
 * Created by xiaofan on 4/13/15.
 */
public class CreateMsg extends Message {

  public CreateMsg(int src, int dst) {
    super(src, dst);
  }

  public String toString () {
    return super.toString() + "Create()";
  }
}

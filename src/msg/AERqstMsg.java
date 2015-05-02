package msg;

/**
 * First of the three message for anti-entropy process
 * Ask for receiver's information
 */
public class AERqstMsg extends Message{
  public AERqstMsg (int s, int d) {
    super(s, d);
  }

  public String toString() {
    return super.toString() + "Anti_Entropy_Request()";
  }
  @Override
  public boolean isAEmsg(){
	  return true;
  }
}


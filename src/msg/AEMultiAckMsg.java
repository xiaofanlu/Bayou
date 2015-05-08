package msg;
/**
 * This anti-entropy multiple ack message return a all the AEAckMsg
 * to the receiver. The receiver can decide on the first message inserted
 * in the writelog
 */

import java.util.ArrayList;
import java.util.List;

public class AEMultiAckMsg extends Message {
	public List<Message> msgList;
	public AEMultiAckMsg(int s, int d){
		super(s,d);
		msgList = new ArrayList<Message>();
	}
	public void addMsg(Message msg){
		msgList.add(msg);
	}
	public boolean isEmpty(){
		return msgList.isEmpty();
	}
	public Message getFirst(){
		if(msgList.isEmpty()){
			return null;
		}else{
			return msgList.get(0);
		}
	}
	@Override
	public boolean isAEmsg(){
		return true;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString() + "Multiple_Anti_entrophy_ack\n");
		List<Message> temp = new ArrayList<Message>(msgList);
		for(Message msg : temp){
			sb.append(msg.toString() + "\n");
		}
		return sb.toString();
	}
}

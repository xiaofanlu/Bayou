package util;

import command.ClientCmd;
import command.ServerCmd;
import command.Del;
import command.Put;
import command.Get;
import command.Command;

import java.util.Stack;

public class UndoLog {
	Stack<Write> Log;
	
	public UndoLog(){
		Log = new Stack<Write>();
	}
	
	public Write lastEntry(){// read the undo log of the last Write
		return Log.peek();
	}
	
	public boolean isEmpty(){
		return Log.isEmpty();
	}
	
	public void push(Write wr){
		Log.push(wr);
	}
	
	public Write pop(){
		return Log.pop();
	}
	
	
}

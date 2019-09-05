package oplog.entity;

import com.mongodb.DBObject;
import lombok.Data;

@Data
public class OplogEvent {
	
	private String strIP;
	private int nPort;
	private String strWorkId;
	private DBObject dbObject;

	public OplogEvent(){
	}
	
	public OplogEvent(String strIP, int nPort, String strWorkId, DBObject dbObject){
		this.strIP = strIP;
		this.nPort = nPort;
		this.strWorkId = strWorkId;
		this.dbObject = dbObject;
	}

	public void from(OplogEvent oplogEvent){
		this.strIP = oplogEvent.getStrIP();
		this.nPort = oplogEvent.getNPort();
		this.strWorkId = oplogEvent.getStrWorkId();
		this.dbObject = oplogEvent.getDbObject();
	}

	public String toString(){
		return dbObject.toString();
	}

}

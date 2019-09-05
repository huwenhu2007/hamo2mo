package oplog.entity;

import lombok.Data;
import org.apache.log4j.Logger;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 位置信息对账
 */
@Data
public class OplogPosition {

	private static Logger logger = Logger.getLogger(OplogPosition.class);
 	
	private String strIP;

	private int nPort;

	private String strWorkId;

	private String strSign;

	private int nTime;

	private int nIncrement;
	
	public OplogPosition(){
		
	}
	
	public OplogPosition(String strIP, int nPort, String strWorkId, String strSign, int nTime, int nIncrement) {
		this.strIP=strIP;
		this.nPort=nPort;
		this.strWorkId = strWorkId;
		this.strSign=strSign;
		this.nTime=nTime;
		this.nIncrement=nIncrement;
	}

	public void from(OplogPosition oplogPosition){
		this.strIP = oplogPosition.getStrIP();
		this.nPort = oplogPosition.getNPort();
		this.strWorkId = oplogPosition.getStrWorkId();
		this.strSign = oplogPosition.getStrSign();
		this.nTime = oplogPosition.getNTime();
		this.nIncrement=oplogPosition.getNIncrement();
	}

	@Override
	public String toString() {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		StringBuffer sb = new StringBuffer();
		sb.append("strIP:").append(strIP).append(";")
			.append("nPort:").append(nPort).append(";")
			.append("strWorkId:").append(strWorkId).append(";")
			.append("strSign:").append(strSign).append(";")
			.append("nTime:").append(nTime).append(";")
			.append("nIncrement:").append(nIncrement).append(";")
			.append("strTime:").append(sdf.format(new Long(nTime) * 1000)).append(";");
		return sb.toString();
	}

	/**
	 * 转换为日期信息
	 * @return
	 */
	public String toDateString(SimpleDateFormat sdf){
		StringBuilder sb = new StringBuilder();
		sb.append(sdf.format( new Date((long)nTime * 1000L))).append("|").append(nIncrement);
		return sb.toString();
	}
	
}

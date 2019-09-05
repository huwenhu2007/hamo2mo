package oplog;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import exception.ZKException;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.StringUtils;
import utils.PositionUtils;
import zk.ZKClient;

import java.util.HashSet;
import java.util.List;

@Data
@NoArgsConstructor
public class MongoConfig {
	private String strWorkerId;
	private String strIP;
	private int nPort;
	private String strUserName;
	private String strPassword;
	private String strOplogModel;
	private int nVersion;
	private int nPositionEnable;
	private int nTime;
	private int nIncrement;
	private JSONObject zkPosition;
	private int nDebug;
	
	private JSONArray strDMLTargetJSONArray;
	private JSONArray arrOplogEventFilter;
	private JSONArray arrOplogDataFilter;

	public void build(JSONObject jsonObject, JSONObject pJSONObject){
		this.strWorkerId = jsonObject.getString("strWorkerId");
		this.strIP = jsonObject.getString("strDBIP");
		this.nPort = jsonObject.getIntValue("nDBPort");
		this.strUserName = jsonObject.getString("strUserName");
		this.strPassword = jsonObject.getString("strPassWord");
		this.strOplogModel = jsonObject.getString("strOplogModel");
		this.nVersion = jsonObject.getIntValue("nVersion");
		this.nDebug = jsonObject.getIntValue("nDebug");
		this.nTime = jsonObject.getIntValue("nTime");
		this.nIncrement = jsonObject.getIntValue("nIncrement");
		this.nPositionEnable = jsonObject.getIntValue("nPositionEnable");
		this.zkPosition = pJSONObject;
		this.strDMLTargetJSONArray = jsonObject.getJSONArray("strDMLTargetJSONArray");
		this.arrOplogEventFilter = jsonObject.getJSONArray("arrOplogEventFilter");
		this.arrOplogDataFilter = jsonObject.getJSONArray("arrOplogDataFilter");
	}

	public MongoConfig(JSONObject jsonSourceData, String strWorkerPath) throws ZKException{
		JSONObject pJSONObject = null;
		// 生成目标配置JSON对象
		JSONArray jsonArray = new JSONArray();
		List<JSONObject> listChild = ZKClient.getInstance().getChildNodeJSONDataList(strWorkerPath);
		int nListChildSize = listChild.size();
		for(int j = 0;j < nListChildSize;j++) {
			JSONObject jsonObjectChild = listChild.get(j);
			String nodeName = jsonObjectChild.getString( "nodeName" );
			if(StringUtils.equals("state", nodeName)){
				JSONObject rateJSONObject = jsonObjectChild.getJSONObject( "rate" );
				pJSONObject = PositionUtils.getPositionFromZK(rateJSONObject);
			} else {
				jsonArray.add( jsonObjectChild );
			}
		}
		jsonSourceData.put("strDMLTargetJSONArray", jsonArray);
		// 组装配置信息
		build(jsonSourceData, pJSONObject);
	}

	public boolean isDebug(){
		return nDebug == 1 ? true : false;
	}

	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(strIP).append("|");
		sb.append(nPort).append("|");
		sb.append(strWorkerId);
		return sb.toString();
	}
}

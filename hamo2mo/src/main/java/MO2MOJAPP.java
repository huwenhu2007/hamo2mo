
import com.alibaba.fastjson.JSONObject;
import exception.ZKException;
import log.LogConfiguration;
import oplog.task.WorkerMoniterThread;
import org.apache.commons.lang.StringUtils;
import utils.OperatingSystemUtils;
import utils.Utilitys;
import org.apache.log4j.Logger;
import zk.NodeNameConstans;
import zk.NodeType;
import zk.ZKClient;
import java.nio.charset.Charset;
import java.util.concurrent.*;

/**
 * 同步启动类
 */
public class MO2MOJAPP {
	
	private static Logger logger = Logger.getLogger(MO2MOJAPP.class);

	/**
	 * 设置为单例
	 */
	private MO2MOJAPP(){}

	private static class MO2MOJAPPSingle{
		public static MO2MOJAPP mo2mojapp = new MO2MOJAPP();
	}

	public static MO2MOJAPP getInstance(){
		return MO2MOJAPPSingle.mo2mojapp;
	}

	/**
	 * 启动任务
	 * @throws Exception
	 */
	public void start() throws Exception {
		// main函数入口java文件编码
		logger.info("file.encoding=" + System.getProperty("file.encoding"));
		// 转换为字节数组默认编码
		logger.info("Default Charset=" + Charset.defaultCharset());
		// 转换为字节数组使用编码
 		logger.info("Default Charset in Use=" + OperatingSystemUtils.getInstance().getDefaultCharSet());
 		// 判断进程是否已经启动
 		if(OperatingSystemUtils.getInstance().checkJavaAppIsRuning(OperatingSystemUtils.getInstance().getSelfPID(),OperatingSystemUtils.getInstance().getProcessName())){
 			logger.error(OperatingSystemUtils.getInstance().getProcessName() + " already runing");
			return;
		}
		// 获取配置信息
		String strConfigJSON = Utilitys.getJSONFileData("/config.json");
		if(StringUtils.isBlank(strConfigJSON)){
			throw new RuntimeException(String.format("% 配置信息不存在", "config.properties"));
		}
 		JSONObject jsonConfig = JSONObject.parseObject(strConfigJSON);
		// 创建节点信息
		createProcessNode(jsonConfig);

		// 启动节点线程池（监控线程和修复线程）
		WorkerMoniterThread workerMoniterThread=new WorkerMoniterThread(jsonConfig);
		ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
		scheduledExecutorService.scheduleWithFixedDelay( workerMoniterThread, 0, 1, TimeUnit.MINUTES );

		logger.info("==============================");
		logger.info( String.format( "%s-%s 启动完毕" , OperatingSystemUtils.getInstance().getProcessName(), OperatingSystemUtils.getInstance().getSelfPID()) );
		logger.info("==============================");

	}

	/**
	 * 创建爱你进程使用的节点
	 * @param jsonConfig
	 * @throws ZKException
	 */
	private void createProcessNode(JSONObject jsonConfig) throws Exception{
		// zk集群信息
		String strZKClientDomain = jsonConfig.getString("strZKClientDomain");
		// zk根节点
		String strRootNodeName = jsonConfig.getString("strRootNodeName");
		// 连接zk集群
		ZKClient.getInstance().startZK(strZKClientDomain);
		// 创建状态节点
		createStateNode(strRootNodeName, NodeNameConstans.RUNNABLE_NODE.getName(), NodeType.PERSISTENT);
		createStateNode(strRootNodeName, NodeNameConstans.READY_NODE.getName(), NodeType.PERSISTENT);
		createStateNode(strRootNodeName, NodeNameConstans.PROCESS_LIFE_MONITER_NODE.getName(), NodeType.PERSISTENT);
		// 创建进程节点
		String strPersiNode = createProcessNode(strRootNodeName, NodeNameConstans.RUNNABLE_NODE.getName(), NodeType.PERSISTENT);
		String strLifeNode = createProcessNode(strRootNodeName, NodeNameConstans.PROCESS_LIFE_MONITER_NODE.getName(), NodeType.TEMPORARY);

		listenerNode(strPersiNode, strLifeNode);

		nodeStarted(strRootNodeName, NodeNameConstans.RUNNABLE_NODE.getName());
	}

	/**
	 * 创建状态节点
	 * @param strRootNodeName
	 * @param strStateNode
	 * @param nNodeType			0 持久节点，1 临时节点
	 * @throws ZKException
	 */
	private String createStateNode(String strRootNodeName, String strStateNode, NodeType nNodeType) throws ZKException {
		String nodePath = new StringBuilder("/").append(strRootNodeName).append("/").append( strStateNode).toString();
		createNode(nodePath, "", nNodeType);
		return nodePath;
	}

	/**
	 * 创建进程节点
	 * @param strRootNodeName
	 * @param strStateNode
	 * @param nNodeType
	 * @throws ZKException
	 */
	private String createProcessNode(String strRootNodeName, String strStateNode, NodeType nNodeType) throws ZKException {
		String nodePath = new StringBuilder("/").append(strRootNodeName)
				.append("/").append( strStateNode)
				.append( "/" ).append( OperatingSystemUtils.getInstance().getProcessName() ).append( "-" ).append( OperatingSystemUtils.getInstance().getSelfPID() ).toString();
		createNode(nodePath, "", nNodeType);
		return nodePath;
	}

	/**
	 * 创建节点
	 * @param nodePath
	 * @param strMessage
	 * @param nNodeType
	 * @throws ZKException
	 */
	private void createNode(String nodePath, String strMessage, NodeType nNodeType) throws ZKException {
		switch (nNodeType) {
			case PERSISTENT :
				ZKClient.getInstance().createPersistentNode(nodePath, strMessage);
				return;
			case TEMPORARY :
				ZKClient.getInstance().createTemporaryNode(nodePath, strMessage);
				return;
		}
		throw new ZKException( String.format(  "%s 节点类型不存在" , nodePath));
	}

	private void listenerNode(String strNodePath, String strLifeNodePath) throws Exception{
		ZKClient.getInstance().onListenerChildren(strNodePath);
		ZKClient.getInstance().addSessionListener(strLifeNodePath);
	}

	private void nodeStarted(String strRootNodeName, String strStateNode) throws Exception{
		String nodePath = new StringBuilder("/").append(strRootNodeName)
				.append("/").append( strStateNode)
				.append( "/" ).append( OperatingSystemUtils.getInstance().getProcessName() ).append( "-" ).append( OperatingSystemUtils.getInstance().getSelfPID() ).toString();
		JSONObject jsonObject = new JSONObject(  );
		jsonObject.put( "state", "running" );
		ZKClient.getInstance().updateNode(nodePath, jsonObject.toString());
	}

	public static void main(String[] args) {
		// 设置log4j日志配置
		LogConfiguration.initLog(OperatingSystemUtils.getInstance().getProcessName());
		// 获取任务启动对象
		MO2MOJAPP m = MO2MOJAPP.getInstance();
		try {
			// 启动任务
			m.start();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
}

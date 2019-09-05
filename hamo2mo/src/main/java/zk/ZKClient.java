package zk;

import com.alibaba.fastjson.JSONObject;
import exception.ZKException;
import oplog.MongoConfig;
import oplog.OplogWorkerManager;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import java.util.ArrayList;
import java.util.List;

public class ZKClient {
	
	private static final Logger logger = Logger.getLogger(ZKClient.class);
	
	public static ZKClient getInstance() {
		return ZKClientHolder.instance;
	}
	
	private static class ZKClientHolder {
		private static ZKClient instance = new ZKClient();
	}
	
	private ZKClient() {}
	
	CuratorFramework client = null;
	
	/**
	 * 启动zk客户端
	 * @param strClientDomain	zk服务器的ip和端口配置
	 */
	public void startZK(String strClientDomain){
		// 设置zk的重试规则（按照1秒进行渐进重试，最多重试3次）
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		// 使用流式的方式创建客户端
		client = CuratorFrameworkFactory.builder()
                .connectString(strClientDomain)
                .sessionTimeoutMs(30000)
                .connectionTimeoutMs(10000)
                .retryPolicy(retryPolicy)
                .build();
		// 开启客户端
        client.start();
	}
	
	public boolean isStarted(){
		if(client == null){
			return false;
		}
		return client.isStarted();
	}

	public void stop()
	{
		if(client == null)
		{
			return;
		} else
		{
			removeSessionListener();
			client.close();
			return;
		}
	}

	// zk会话监听
	SessionConnectionListener sessionConnectionListener;

	public void addSessionListener(String strLifeNodePath)
	{
		sessionConnectionListener = new SessionConnectionListener(strLifeNodePath);
		client.getConnectionStateListenable().addListener(sessionConnectionListener);
	}

	public void removeSessionListener()
	{
		if(sessionConnectionListener == null)
		{
			return;
		} else
		{
			client.getConnectionStateListenable().removeListener(sessionConnectionListener);
			return;
		}
	}

	/**
	 * 获取子节点信息
	 * @param strNode      节点路径
	 * @return
	 * @throws Exception
	 */
	public List<String> getChildNode(String strNode) throws ZKException {
		try {
			List<String> list = client.getChildren().forPath( strNode );
			return list;
		} catch (Exception e){
			throw new ZKException( e );
		}
	}

	/**
	 * 获取节点数据
	 * @param strNode		节点路径
	 * @return
	 * @throws Exception
	 */
	public String getNodeStrData(String strNode) throws ZKException {
		try {
			byte[] bData = client.getData().forPath(strNode);
			String strData = new String(bData);
			return strData;
		} catch (Exception e){
			throw new ZKException( e );
		}
	}


	/**
	 * 获取节点数据并返回json
	 * @param strNode
	 * @return
	 * @throws Exception
	 */
	public JSONObject getNodeJSONData(String strNode) throws ZKException {
		String strData = getNodeStrData(strNode);
		JSONObject jsonData = JSONObject.parseObject(strData);
		return jsonData;
	}

	/**
	 * 获取子节点字符串内容
	 * @param strNode		节点名称
	 * @return
	 * @throws Exception
	 */
	public List<String> getChildNodeStrDataList(String strNode) throws Exception {
		List<String> list = getChildNode(strNode);
		int nListSize = list.size();
		// 添加返回字符串数据
		ArrayList<String> childNodeStrDataList = new ArrayList<>();
		for(int i = 0;i < nListSize;i++){
			String strChildNodeName = list.get(i);
			String strNodePath = strNode + "/" + strChildNodeName;
			String strChildNodeStrData = getNodeStrData(strNodePath);
			childNodeStrDataList.add(strChildNodeStrData);
		}
		return childNodeStrDataList;
	}

	/**
	 * 获取子节点JSON数据
	 * @param strNode        节点路径
	 * @return
	 * @throws Exception
	 */
	public List<JSONObject> getChildNodeJSONDataList(String strNode) throws ZKException {
		List<String> list = getChildNode(strNode);
		int nListSize = list.size();
		// 添加返回JSON数据
		ArrayList<JSONObject> childNodeJSONDataList = new ArrayList<>();
		for(int i = 0;i < nListSize;i++){
			String strChildNodeName = list.get(i);
			String strNodePath = strNode + "/" + strChildNodeName;
			JSONObject strChildNodeJSONData = getNodeJSONData(strNodePath);
			strChildNodeJSONData.put( "nodeName",  strChildNodeName);
			childNodeJSONDataList.add(strChildNodeJSONData);
		}
		return childNodeJSONDataList;
	}

	/**
	 * 创建临时节点
	 * @param strPath		节点路径
	 * @param strMessage	节点信息
	 * @throws Exception
	 */
	public void createTemporaryNode(String strPath, String strMessage) throws ZKException {
		try {
			if(isExist(strPath)){
				return ;
			}
			client.create()
				.creatingParentsIfNeeded()
				.withMode(CreateMode.EPHEMERAL)
				.forPath(strPath,strMessage.getBytes());
		} catch(Exception e){
			throw new ZKException( e );
		}
	}
	
	/**
	 * 创建永久节点
	 * @param strPath		节点路径
	 * @param strMessage	节点信息
	 * @throws Exception
	 */
	public void createPersistentNode(String strPath, String strMessage) throws ZKException {
		try {
			if (isExist( strPath )) {
				return;
			}
			client.create()
					.creatingParentsIfNeeded()
					.withMode( CreateMode.PERSISTENT )
					.forPath( strPath, strMessage.getBytes() );
		} catch(Exception e){
			throw new ZKException( e );
		}
	}
	
	/**
	 * 判断节点是否存在
	 * @param strPath		节点路径
	 * @return
	 * @throws Exception
	 */
	public boolean isExist(String strPath) throws Exception {
		Stat stat = client.checkExists().forPath(strPath);
		if(stat == null){
			return false;
		}
		return true;
	}
	
	/**
	 * 修改节点信息
	 * @param strPath		节点路径
	 * @param strMessage	信息
	 * @throws Exception
	 */
	public void updateNode(String strPath, String strMessage) throws Exception {
		Stat stat = new Stat();
		client.getData().storingStatIn(stat).forPath(strPath);
		client.setData().withVersion(stat.getVersion()).forPath(strPath, strMessage.getBytes());
	}
	
	/**
	 * 删除节点信息
	 * @param strPath		节点路径
	 * @throws Exception
	 */
	public void deleteNode(String strPath) throws Exception {
		if(!isExist(strPath)){
			return ;
		}
		
		client.delete()
			.guaranteed()				// 删除失败，则客户端会持续删除
			.deletingChildrenIfNeeded()
			.withVersion(-1)			// 直接删除，无需考虑版本
			.forPath(strPath);
	}
	
	public void onListenerChildren(String strPath) throws Exception {
		// 子节点监听
		final PathChildrenCache pccache = new PathChildrenCache(client,strPath,true);
		// 启动监听
		pccache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
		// 添加监听事件
		pccache.getListenable().addListener(new PathChildrenCacheListener() {
			
			@Override
			public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event)
					throws Exception {
				
				// 事件类型
				switch (event.getType()) {
					// 添加
					case CHILD_ADDED:
						logger.info("[path:"+event.getData().getPath()+"] 添加节点");
						break ;
					// 修改
					case CHILD_UPDATED:
						// 获取节点对象
						ChildData cd = event.getData();
						// 获取节点路径
						String path = cd.getPath();
						// 获取节点内容
						byte[] bData = cd.getData();
						String strData = new String(bData);
						JSONObject jsonData = JSONObject.parseObject( strData );
						String runState = jsonData.getString( "runState" );

						if(NodeState.START.getState().equals( runState )){
							try {
								MongoConfig mongoConfig = new MongoConfig( jsonData, path );
								OplogWorkerManager.getInstance().startWork(mongoConfig);
							} catch(Exception e){
								// 设置节点状态为失败
								jsonData.put( "runState", NodeState.EXCEPTION.getState() );
								jsonData.put( "nPositionEnable", 0 );
								jsonData.remove( "strDMLTargetJSONArray" );
								updateNode(path, jsonData.toString());
								logger.error( String.format( "%s 启动失败： %s", path, jsonData.toString() ), e );
								break ;
							}

							// 任务启动成功
							jsonData.put( "runState", NodeState.RUNNING.getState() );
							jsonData.put( "nPositionEnable", 0 );
							jsonData.remove( "strDMLTargetJSONArray" );
							updateNode(path, jsonData.toString());
						}

						else if(NodeState.STOP.getState().equals( runState )){
							try {
								OplogWorkerManager.getInstance().stopWork( jsonData , path);
							} catch(Exception e){
								// 设置节点状态为失败
								jsonData.put( "runState", NodeState.EXCEPTION.getState() );
								jsonData.put( "nPositionEnable", 0 );
								jsonData.remove( "strDMLTargetJSONArray" );
								updateNode(path, jsonData.toString());
								logger.error( String.format( "%s 停止失败： %s", path, jsonData.toString() ), e );
								break ;
							}

							// 任务停止成功
							jsonData.put( "runState", NodeState.SHUT.getState() );
							jsonData.put( "nPositionEnable", 0 );
							jsonData.remove( "strDMLTargetJSONArray" );
							updateNode(path, jsonData.toString());
						}

						else if(NodeState.RESTART.getState().equals( runState )){
							try {
								OplogWorkerManager.getInstance().stopWork( jsonData , path);

								MongoConfig mongoConfig = new MongoConfig( jsonData, path );
								OplogWorkerManager.getInstance().startWork(mongoConfig);
							} catch(Exception e){
								// 设置节点状态为失败
								jsonData.put( "runState", NodeState.EXCEPTION.getState() );
								jsonData.put( "nPositionEnable", 0 );
								jsonData.remove( "strDMLTargetJSONArray" );
								updateNode(path, jsonData.toString());
								logger.error( String.format( "%s 重启失败： %s", path, jsonData.toString() ), e );
								break ;
							}

							// 任务重启成功
							jsonData.put( "runState", NodeState.RUNNING.getState() );
							jsonData.put( "nPositionEnable", 0 );
							jsonData.remove( "strDMLTargetJSONArray" );
							updateNode(path, jsonData.toString());
						}

						break;
					// 删除
					case CHILD_REMOVED:
						logger.info("[path:"+event.getData().getPath()+"] 删除节点");
						// 清理集合中的对象
						String strDeleteNodeData = new String(event.getData().getData());
						JSONObject jsonDeleteNode = JSONObject.parseObject( strDeleteNodeData );
						try {
							OplogWorkerManager.getInstance().stopWork( jsonDeleteNode , event.getData().getPath());
							OplogWorkerManager.getInstance().removeWork( jsonDeleteNode );
						} catch(Exception e){
							// 设置节点状态为失败
							jsonDeleteNode.put( "runState", NodeState.EXCEPTION.getState() );
							updateNode(event.getData().getPath(), jsonDeleteNode.toString());
							logger.error( String.format( "%s 停止失败： %s", event.getData().getPath(), jsonDeleteNode.toString() ), e );
							break ;
						}
						break;
					// 连接暂停
					case CONNECTION_SUSPENDED:
						logger.info("暂停连接节点");
						break;
					case CONNECTION_LOST:
						logger.info("节点连接丢失");
						break;
					default:
						break;
				}
				
			}
		});

	}

	public static void main(String[] args) {
		ZKClient zkClient = new ZKClient();
		zkClient.startZK("127.0.0.1:2181");
		try {
			
			zkClient.onListenerChildren("/M2M4J_6.0");
			for(int i = 0;i < 10;i++){
				if(zkClient.isExist("/M2M4J_6.0/123")){
					zkClient.updateNode("/M2M4J_6.0/123", "22222");
				} else {
					zkClient.createPersistentNode("/M2M4J_6.0/123", "");
				}
				Thread.sleep(1000);
				System.out.println("-------------");
			}
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}

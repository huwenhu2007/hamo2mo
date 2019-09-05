package cn.com.manager.zk;

import cn.com.manager.exception.ZKException;
import cn.com.manager.nodeswitch.ThreadPoolFactory;
import cn.com.manager.utils.OperatingSystemUtils;
import cn.com.manager.zk.constans.NodeNameConstans;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class ZKClient {
	

	@Value( "#{${zk.sleepTimeMs}}" )
	private Integer sleepTimeMs;
	@Value( "#{${zk.maxRetries}}" )
	private Integer maxRetries;
	@Value( "${zk.strClientDomain}" )
	private String strClientDomain;
	@Value( "#{${zk.sessionTimeoutMs}}" )
	private Integer sessionTimeoutMs;
	@Value( "#{${zk.connectionTimeoutMs}}" )
	private Integer connectionTimeoutMs;

	@Autowired
	ThreadPoolFactory threadPoolFactory;
	@Autowired
	OperatingSystemUtils operatingSystemUtils;

	private CuratorFramework client = null;
	private LeaderLatch leaderLatch = null;
	private String leadNode;
	private String slaveId;
	
	/**
	 * 启动zk客户端
	 */
	public void startZK() throws Exception{
		// 设置zk的重试规则（按照1秒进行渐进重试，最多重试3次）
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(sleepTimeMs, maxRetries);
		// 使用流式的方式创建客户端
		client = CuratorFrameworkFactory.builder()
                .connectString(strClientDomain)
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .retryPolicy(retryPolicy)
                .build();
		// 开启客户端
        client.start();

		leadNode = new StringBuilder( "/" ).append( NodeNameConstans.ROOT.getName() ).append( "/" ).append( NodeNameConstans.LEAD.getName() ).toString();
		slaveId = new StringBuilder( operatingSystemUtils.getIp() ).append( "-" ).append( operatingSystemUtils.getProcessName() ).toString();
	}

	public CuratorFramework getClient(){
		return client;
	}

	public LeaderLatch getLeaderLatch(){
		return leaderLatch;
	}
	
	public boolean isStarted(){
		if(client == null){
			return false;
		}
		return client.isStarted();
	}

	public void stop() throws IOException {
		if(client == null)
		{
			return;
		} else
		{
			removeSessionListener();
			client.close();
			leaderLatch.close();
			return;
		}
	}

	// zk会话监听
	SessionConnectionListener sessionConnectionListener;

	public void addSessionListener(String strLifeNodePath)
	{
		sessionConnectionListener = new SessionConnectionListener();
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
		if(StringUtils.isBlank( strData )){
			return new JSONObject(  );
		}
		JSONObject jsonData = null;
		try {
			jsonData = JSONObject.parseObject( strData );
		} catch(Exception e){
			jsonData = new JSONObject(  );
			jsonData.put( "noJSONType", strData );
		}
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
				.withMode( CreateMode.EPHEMERAL)
				.forPath(strPath,strMessage.getBytes());
		} catch(Exception e){
			throw new ZKException( e );
		}
	}

	public boolean createTemporaryNodeLock(String strPath) {
		try {
			client.create()
					.creatingParentsIfNeeded()
					.withMode( CreateMode.EPHEMERAL )
					.forPath( strPath, "".getBytes() );
			return true;
		} catch(Exception e){
			return false;
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

	public boolean createPersistentNodeLock(String strPath, String strMessage) {
		try {
			client.create()
					.creatingParentsIfNeeded()
					.withMode( CreateMode.PERSISTENT )
					.forPath( strPath, strMessage.getBytes() );
			return true;
		} catch(Exception e){
			return false;
		}
	}
	
	/**
	 * 判断节点是否存在
	 * @param strPath		节点路径
	 * @return
	 * @throws Exception
	 */
	public boolean isExist(String strPath) throws ZKException {
		try {
			Stat stat = client.checkExists().forPath( strPath );
			if (stat == null) {
				return false;
			}
			return true;
		} catch(Exception e){
			throw new ZKException( e );
		}
	}
	
	/**
	 * 修改节点信息
	 * @param strPath		节点路径
	 * @param strMessage	信息
	 * @throws Exception
	 */
	public void updateNode(String strPath, String strMessage) throws ZKException {
		try {
			Stat stat = new Stat();
			client.getData().storingStatIn(stat).forPath(strPath);
			client.setData().withVersion(stat.getVersion()).forPath(strPath, strMessage.getBytes());
		} catch(Exception e){
			throw new ZKException( e );
		}
	}
	
	/**
	 * 删除节点信息
	 * @param strPath		节点路径
	 * @throws Exception
	 */
	public void deleteNode(String strPath) throws ZKException {
		try {
			if(!isExist(strPath)){
				return ;
			}

			client.delete()
				.guaranteed()				// 删除失败，则客户端会持续删除
				.deletingChildrenIfNeeded()
				.withVersion(-1)			// 直接删除，无需考虑版本
				.forPath(strPath);
		} catch(Exception e){
			throw new ZKException( e );
		}
	}

	public void onListenerLifeMoniterChildren() {
		String nodePath = new StringBuilder( "/" ).append( NodeNameConstans.ROOT.getName() ).append( "/" ).append( NodeNameConstans.PROCESS_LIFE_MONITER_NODE.getName() ).toString();
		try {
			onListenerChildren( nodePath );
		} catch(Exception e){
			log.error("{}监听添加异常:{}", nodePath, e.getMessage(), e);
		}
	}

	public void onListenerChildren(String strPath) throws Exception {
		// 子节点监听
		final PathChildrenCache pccache = new PathChildrenCache(client,strPath,true);
		// 启动监听
		pccache.start( PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
		// 添加监听事件
		pccache.getListenable().addListener(new PathChildrenCacheListener() {
			
			@Override
			public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event)
					throws Exception {
				
				// 事件类型
				switch (event.getType()) {
					// 添加
					case CHILD_ADDED:
						log.info("[path:"+event.getData().getPath()+"] 添加节点");
						break ;
					// 修改
					case CHILD_UPDATED:
						log.info("[path:"+event.getData().getPath()+"] 修改节点");
						break;
					// 删除
					case CHILD_REMOVED:
						log.info("[path:"+event.getData().getPath()+"] 删除节点");
						String path = event.getData().getPath();
						threadPoolFactory.execute( path );
						break;
					// 连接暂停
					case CONNECTION_SUSPENDED:
						log.info("暂停连接节点");
						break;
					case CONNECTION_LOST:
						log.info("节点连接丢失");
						break;
					default:
						break;
				}
				
			}
		});

	}

}

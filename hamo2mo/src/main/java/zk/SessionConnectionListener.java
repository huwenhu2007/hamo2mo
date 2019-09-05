package zk;

import oplog.OplogWorkerManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.log4j.Logger;


/**
 * @Author huwenhu
 * @Date 2019/1/30 16:46
 **/
public class SessionConnectionListener implements ConnectionStateListener {

    private final Logger logger = Logger.getLogger(SessionConnectionListener.class);
    private final long OVER_TIME = 60 * 1000;

    private String strLifeNodePath;

    public SessionConnectionListener(String strLifeNodePath)
    {
        this.strLifeNodePath = strLifeNodePath;
    }

    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState)
    {
        if(connectionState == ConnectionState.LOST)
        {
            logger.info("[负载均衡失败]zk session超时");
            long lStartTime = System.currentTimeMillis();
            do
            {
                try
                {
                    if((System.currentTimeMillis() - lStartTime) > OVER_TIME){
                        // 超过指定事件未重连成功则停止进程
                        OplogWorkerManager.getInstance().stopProcess(  );
                        break ;
                    }
                    if(curatorFramework.getZookeeperClient().blockUntilConnectedOrTimedOut())
                    {
                        // 重连成功，重建虚拟节点
                        ZKClient.getInstance().createTemporaryNode(strLifeNodePath, "");
                        break;
                    }
                }
                catch(InterruptedException e)
                {
                    logger.error((new StringBuilder()).append("[负载均衡失败]").append(e.getMessage()).toString(), e);
                }
                catch(Exception e)
                {
                    logger.error((new StringBuilder()).append("[负载均衡失败]").append(e.getMessage()).toString(), e);
                }
                try
                {
                    Thread.sleep(20L);
                }
                catch(InterruptedException e) { }
            } while(true);
        }
    }

}

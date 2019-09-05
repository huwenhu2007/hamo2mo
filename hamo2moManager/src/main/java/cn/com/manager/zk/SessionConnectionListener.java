package cn.com.manager.zk;

import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;


/**
 * @Author huwenhu
 * @Date 2019/1/30 16:46
 **/
@Slf4j
public class SessionConnectionListener implements ConnectionStateListener {

    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState)
    {
        if(connectionState == ConnectionState.LOST)
        {
            log.info("zk 连接丢失");
            do
            {
                try
                {
                    if(curatorFramework.getZookeeperClient().blockUntilConnectedOrTimedOut())
                    {
                        log.info("zk 重连成功");
                        break;
                    }
                }
                catch(InterruptedException e)
                {

                    log.error("连接中断异常:{}", e.getMessage(), e);
                }
                catch(Exception e)
                {
                    log.error("连接异常:{}", e.getMessage(), e);
                }
                try
                {
                    Thread.sleep(100L);
                }
                catch(InterruptedException e) { }
            } while(true);
        }
    }

}

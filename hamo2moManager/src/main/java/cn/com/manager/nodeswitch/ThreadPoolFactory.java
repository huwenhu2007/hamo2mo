package cn.com.manager.nodeswitch;

import cn.com.manager.zk.ZKClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

/**
 * @Author huwenhu
 * @Date 2019/8/29 18:09
 **/
@Component
public class ThreadPoolFactory {

    @Value( "#{${threadPool.corePoolSize}}" )
    private int corePoolSize;
    @Value( "#{${threadPool.maxPoolSize}}" )
    private int maxPoolSize;
    @Value( "#{${threadPool.queueSize}}" )
    private int queueSize;

    @Value( "#{${scheduled.scheduledTime}}" )
    private int scheduledTime;
    @Value( "#{${scheduled.delayTime}}" )
    private int delayTime;

    @Autowired
    private ZKClient zkClient;
    @Autowired
    private NodeSwitchBean nodeSwitchBean;

    ExecutorService executor = null;

    ScheduledExecutorService scheduledExecutorService = null;

    public void init(){
        executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, 60L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(queueSize), new ThreadPoolExecutor.CallerRunsPolicy());

        scheduledExecutorService = Executors.newScheduledThreadPool( 1 );
        scheduledExecutorService.scheduleWithFixedDelay( new ScheduleNodeSwitchTask( zkClient, nodeSwitchBean ), delayTime, scheduledTime, TimeUnit.SECONDS );
    }

    public void execute(String tempNodePath){
        executor.execute( new NodeSwitchTask(tempNodePath, zkClient, nodeSwitchBean) );
    }

}

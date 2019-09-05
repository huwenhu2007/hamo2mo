package cn.com.manager.listenter;

import cn.com.manager.exception.ZKException;
import cn.com.manager.nodeswitch.ThreadPoolFactory;
import cn.com.manager.nodetree.NodeTreeBean;
import cn.com.manager.zk.ZKClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

/**
 * @Author huwenhu
 * @Date 2019/8/26 9:54
 **/
@Component
public class AppStartedListener implements ApplicationListener<ApplicationStartedEvent>{

    @Autowired
    ZKClient zkClient;
    @Autowired
    NodeTreeBean nodeTreeBean;
    @Autowired
    ThreadPoolFactory threadPoolFactory;

    @Override
    public void onApplicationEvent(ApplicationStartedEvent applicationStartedEvent) {
        threadPoolFactory.init();

        try {
            zkClient.startZK(  );
            zkClient.onListenerLifeMoniterChildren(  );
        } catch (Exception e) {
            throw new RuntimeException( e );
        }

        nodeTreeBean.getTree();
    }
}

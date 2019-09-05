package cn.com.manager.nodeswitch;

import cn.com.manager.exception.ZKException;
import cn.com.manager.zk.ZKClient;
import cn.com.manager.zk.constans.NodeNameConstans;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.TimeUnit;

/**
 * @Author huwenhu
 * @Date 2019/8/29 17:08
 **/
@Slf4j
public class NodeSwitchTask implements Runnable{

    private ZKClient zkClient;
    private String tempNodePath;
    private NodeSwitchBean nodeSwitchBean;

    public NodeSwitchTask(String tempNodePath, ZKClient zkClient, NodeSwitchBean nodeSwitchBean){
        this.tempNodePath = tempNodePath;
        this.zkClient = zkClient;
        this.nodeSwitchBean = nodeSwitchBean;
    }

    public void run(){

        String taskLeadNodePath = new StringBuilder( "/" ).append( NodeNameConstans.ROOT.getName() ).append( "/" ).append( NodeNameConstans.LEAD.getName() )
                .append( "/" ).append( "task" ).toString();

        if(!zkClient.createTemporaryNodeLock( taskLeadNodePath )){
            log.info( "该进程争夺执行权{}失败，不能执行", taskLeadNodePath );
            return ;
        }

        try {

            TimeUnit.MILLISECONDS.sleep( 120 * 1000 );

            nodeSwitchBean.switchNode( zkClient, tempNodePath );

        } catch (Exception e) {
            log.error( "任务切换异常:{}" , e.getMessage(), e );
        } finally {
            try {
                zkClient.deleteNode( taskLeadNodePath );
            } catch (ZKException e) {
                log.error( "权限节点 {} 删除失败:{}", taskLeadNodePath, e.getMessage(), e );
            }
        }

    }

}

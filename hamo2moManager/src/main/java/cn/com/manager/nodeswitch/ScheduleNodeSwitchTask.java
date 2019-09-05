package cn.com.manager.nodeswitch;

import cn.com.manager.exception.ZKException;
import cn.com.manager.zk.ZKClient;
import cn.com.manager.zk.constans.NodeNameConstans;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.StopWatch;
import java.util.List;

/**
 * @Author huwenhu
 * @Date 2019/8/30 15:07
 **/
@Slf4j
public class ScheduleNodeSwitchTask implements Runnable{

    private ZKClient zkClient;
    private NodeSwitchBean nodeSwitchBean;

    public ScheduleNodeSwitchTask(ZKClient zkClient, NodeSwitchBean nodeSwitchBean){
        this.zkClient = zkClient;
        this.nodeSwitchBean = nodeSwitchBean;
    }

    @Override
    public void run() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        String taskLeadNodePath = new StringBuilder( "/" ).append( NodeNameConstans.ROOT.getName() ).append( "/" ).append( NodeNameConstans.LEAD.getName() )
                .append( "/" ).append( "task" ).toString();

        if(!zkClient.createTemporaryNodeLock( taskLeadNodePath )){
            log.info( "该进程争夺执行权{}失败，不能执行", taskLeadNodePath );
            return ;
        }

        try {

            String runnableNodePath = new StringBuilder( "/" ).append( NodeNameConstans.ROOT.getName() ).append( "/" ).append( NodeNameConstans.RUNNABLE_NODE.getName() ).toString();
            List<String> processNodeNameList = zkClient.getChildNode( runnableNodePath );
            if(processNodeNameList.isEmpty()){
                log.info( "{}无进程信息", runnableNodePath );
                stopWatch.stop();
                log.info( "ScheduleNodeSwitchTask run time: {}", stopWatch.getTime() );
                return ;
            }
            processNodeNameList.stream().forEach( processNodeName -> {
                String processNodePath = new StringBuilder( runnableNodePath ).append( "/" ).append( processNodeName ).toString();
                try {

                    String lifeMoniterProcessNodePath = processNodePath.replace( NodeNameConstans.RUNNABLE_NODE.getName(), NodeNameConstans.PROCESS_LIFE_MONITER_NODE.getName() );
                    if(zkClient.isExist( lifeMoniterProcessNodePath )){
                        return ;
                    }
                    JSONObject jsonObjectProcess = zkClient.getNodeJSONData( processNodePath );
                    if(!jsonObjectProcess.containsKey( "checkStopNum" )){
                        jsonObjectProcess.put( "checkStopNum", 0 );
                    }
                    int checkStopNum = jsonObjectProcess.getIntValue( "checkStopNum" );
                    if(checkStopNum < 3){
                        checkStopNum ++;
                        jsonObjectProcess.put( "checkStopNum", checkStopNum );
                        zkClient.updateNode( processNodePath, jsonObjectProcess.toString() );
                        return ;
                    }
                    nodeSwitchBean.switchNode( zkClient, lifeMoniterProcessNodePath);

                } catch (Exception e) {
                    log.error( "节点切换异常:{}", e.getMessage(), e );
                }

            } );

        } catch(Exception e){
            log.error( "调度节点检查异常：{}", e.getMessage(), e );
        } finally {
            try {
                zkClient.deleteNode( taskLeadNodePath );
            } catch (ZKException e) {
                log.error( "权限节点 {} 删除失败:{}", taskLeadNodePath, e.getMessage(), e );
            }
        }
        stopWatch.stop();
        log.info( "ScheduleNodeSwitchTask run time: {}ms", stopWatch.getTime() );
    }


}

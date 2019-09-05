package cn.com.manager.nodeswitch;

import cn.com.manager.exception.NodeSwitchException;
import cn.com.manager.zk.ZKClient;
import cn.com.manager.zk.constans.NodeNameConstans;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Author huwenhu
 * @Date 2019/8/30 15:26
 **/
@Slf4j
@Component
public class NodeSwitchBean {

    public void switchNode(ZKClient zkClient, String tempNodePath) throws NodeSwitchException {
        try {
            if (zkClient.isExist( tempNodePath )) {
                log.info( "{}存在，无需处理", tempNodePath );
                return;
            }

            String exceptionProcessNodePath = tempNodePath.replace( NodeNameConstans.PROCESS_LIFE_MONITER_NODE.getName(), NodeNameConstans.RUNNABLE_NODE.getName() );
            String exceptionNodeName = exceptionProcessNodePath.substring( exceptionProcessNodePath.lastIndexOf( "/" ) + 1, exceptionProcessNodePath.length());
            String runnableNodePath = new StringBuilder( "/" ).append( NodeNameConstans.ROOT.getName() ).append( "/" ).append( NodeNameConstans.RUNNABLE_NODE.getName() ).toString();

            List<JSONObject> jsonObjectList = zkClient.getChildNodeJSONDataList( exceptionProcessNodePath );
            if(jsonObjectList.isEmpty()){
                log.error( "{}无任务信息", exceptionProcessNodePath );
                zkClient.deleteNode( exceptionProcessNodePath );
                return ;
            }

            for(JSONObject jsonObject : jsonObjectList){
                int nMinNum = Integer.MAX_VALUE;
                String targetProcessNodePath = "";
                List<String> processNodeNames = zkClient.getChildNode( runnableNodePath );
                for(String processNodeName : processNodeNames){
                    String processNodePath = new StringBuilder( "/" ).append( NodeNameConstans.ROOT.getName() ).append( "/" ).append( NodeNameConstans.RUNNABLE_NODE.getName() ).append( "/" ).append( processNodeName ).toString();
                    if(StringUtils.equals(processNodePath, exceptionProcessNodePath)){
                        continue ;
                    }
                    String lifeMoniterProcessNodePath = processNodePath.replace( NodeNameConstans.RUNNABLE_NODE.getName(), NodeNameConstans.PROCESS_LIFE_MONITER_NODE.getName() );
                    if(!zkClient.isExist( lifeMoniterProcessNodePath )){
                        continue ;
                    }

                    List<String> taskNodePaths = zkClient.getChildNode( processNodePath );
                    int taskNodeNum = taskNodePaths.size();

                    if(nMinNum > taskNodeNum){
                        nMinNum = taskNodeNum;
                        targetProcessNodePath = processNodePath;
                    }
                }
                if(StringUtils.isEmpty( targetProcessNodePath )){
                    log.info( "{}无可用目标节点", runnableNodePath );
                    continue ;
                }

                String exceptionChildNodeName = jsonObject.getString( "nodeName" );
                if(exceptionChildNodeName.contains( "--" )){
                    exceptionChildNodeName = exceptionChildNodeName.substring( 0, exceptionChildNodeName.lastIndexOf( "--" ) );
                }

                String nodeName = new StringBuilder( exceptionChildNodeName ).append( "--" ).append( exceptionNodeName ).toString();
                String nodePath = new StringBuilder( targetProcessNodePath ).append( "/" ).append( nodeName ).toString();
                if(!zkClient.createPersistentNodeLock( nodePath, "" )){
                    log.info( "{}节点创建失败，其它服务已创建", nodePath );
                    continue  ;
                }

                String exceptionTaskNodePath = new StringBuilder( exceptionProcessNodePath ).append( "/" ).append( jsonObject.getString( "nodeName" ) ).toString();
                List<JSONObject> exceptionTargetNodeList = zkClient.getChildNodeJSONDataList( exceptionTaskNodePath );
                for(JSONObject targetJSONObject : exceptionTargetNodeList){
                    String targetNodePath = new StringBuilder( nodePath ).append( "/" ).append( targetJSONObject.getString( "nodeName" ) ).toString();
                    zkClient.createPersistentNode( targetNodePath, targetJSONObject.toString() );
                }

                jsonObject.put( "nodeName",  nodeName);
                jsonObject.put( "strWorkerId",  nodeName);
                jsonObject.put( "runState",  "start");
                zkClient.updateNode( nodePath,  jsonObject.toString());

                zkClient.deleteNode( exceptionTaskNodePath );

                TimeUnit.MILLISECONDS.sleep( 100 );
            }

        } catch (Exception e) {
            log.error( "任务切换异常:" + e.getMessage(), e );
            throw new NodeSwitchException(e);
        }
    }

}

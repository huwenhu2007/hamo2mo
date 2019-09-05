package oplog.task;

import com.alibaba.fastjson.JSONObject;
import com.sun.javafx.binding.StringFormatter;
import moniter.SyncMoniter;
import oplog.OplogWorker;
import oplog.OplogWorkerManager;
import org.apache.log4j.Logger;
import utils.OperatingSystemUtils;
import zk.NodeNameConstans;
import zk.ZKClient;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author huwenhu
 * @Date 2019/8/21 16:20
 **/

public class WorkerMoniterThread extends Thread{

    private Logger logger = Logger.getLogger(WorkerMoniterThread.class);

    private JSONObject jsonConfig;

    public WorkerMoniterThread(JSONObject jsonConfig){
        this.jsonConfig = jsonConfig;
    }

    public void run(){
        ConcurrentHashMap<String, OplogWorker> workerMap = OplogWorkerManager.getInstance().getWorkerMap();
        if(workerMap.isEmpty()){
            logger.info( "no task" );
            return ;
        }
        logger.info( "moniter start" );

        String strRootNodeName = jsonConfig.getString("strRootNodeName");
        String nodePath = new StringBuilder("/").append(strRootNodeName)
                .append("/").append( NodeNameConstans.RUNNABLE_NODE.getName() )
                .append( "/" ).append( OperatingSystemUtils.getInstance().getProcessName() ).append( "-" ).append( OperatingSystemUtils.getInstance().getSelfPID() ).toString();

        ArrayList<String> workerIdList = new ArrayList<>(  );

        synchronized (OplogWorkerManager.getInstance()) {
            Enumeration<String> enumeration = workerMap.keys();
            while(enumeration.hasMoreElements()){
                String strWorkerId = enumeration.nextElement();
                workerIdList.add( strWorkerId );
            }
        }

        int nListSize = workerIdList.size();
        for(int i = 0;i < nListSize;i++){
            String strWorkerId = workerIdList.get( i );
            OplogWorker oplogWorker = workerMap.get( strWorkerId );

            if(Objects.isNull(oplogWorker)){
                logger.info(oplogWorker.getMongoConfig().toString() + " 任务不存在");
                continue ;
            }

            if(oplogWorker.isStarted()){
                /*------任务修复-------*/
                // 维护事件抓取任务
                OplogFetcherTask oplogFetcherTask = oplogWorker.getOplogFetcherTask();
                if(!oplogFetcherTask.isRuning()){
                    // 启动抓取任务
                    try {
                        oplogFetcherTask.startFetch();
                    } catch (Exception e) {
                        logger.error( String.format( "%s 抓取任务修复异常: %s",oplogWorker.getMongoConfig().toString(), e.getMessage()), e );
                    }
                }

                // 维护事件处理器
                ArrayList<OplogEventHandler> eventHandlerList = oplogWorker.getOplogEventHandler();
                if(eventHandlerList != null){
                    int nEventHandlerList = eventHandlerList.size();
                    for(int j = 0;j < nEventHandlerList;j++){
                        OplogEventHandler oplogEventHandler = eventHandlerList.get(j);
                        if(!oplogEventHandler.getOplogEventBEP().isRunning()){
                            try {
                                oplogEventHandler.startDispatch();
                            } catch (Exception e) {
                                logger.error( String.format( "%s-%s 转发任务修复异常: %s",oplogWorker.getMongoConfig().toString(), oplogEventHandler.getStrSign(), e.getMessage()), e );
                            }
                        }
                    }
                }
            } else {
                logger.info(oplogWorker.getMongoConfig().toString() + " 任务未启动，无需维护");
            }

            /*------状态，位置信息保存-------*/
            // 任务状态节点
            String strStateNode = new StringBuilder( nodePath ).append( "/" ).append( strWorkerId ).append( "/" ).append( "state" ).toString();
            // 任务信息
            JSONObject jsonObject = new JSONObject();
            // 获取任务状态信息
            JSONObject stateJsonObject = SyncMoniter.getInstance().getWorkerState( oplogWorker );
            jsonObject.put( "state", stateJsonObject );
            // 获取任务进度信息
            JSONObject rateJsonObject = SyncMoniter.getInstance().getWorkerRate( oplogWorker );
            jsonObject.put( "rate", rateJsonObject );
            // 获取异常信息
            JSONObject exceptionJsonObject = SyncMoniter.getInstance().getExceptionMessage( oplogWorker );
            jsonObject.put( "exception", exceptionJsonObject );
            // 获取队列信息
            JSONObject bufferJsonObject = SyncMoniter.getInstance().getRingBufferData( oplogWorker );
            jsonObject.put( "buffer", bufferJsonObject );
            // 添加任务监控信息
            try {
                if(ZKClient.getInstance().isExist( strStateNode )){
                    ZKClient.getInstance().updateNode( strStateNode, jsonObject.toString() );
                } else {
                    ZKClient.getInstance().createPersistentNode( strStateNode, jsonObject.toString() );
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        logger.info( "moniter end" );

    }

}

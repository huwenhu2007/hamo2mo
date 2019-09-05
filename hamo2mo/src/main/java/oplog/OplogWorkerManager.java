package oplog;

import com.alibaba.fastjson.JSONObject;
import exception.WorkerException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 工作管理者
 * @Author huwenhu
 * @Date 2019/8/16 19:11
 **/

public class OplogWorkerManager {

    private Logger logger = Logger.getLogger(OplogWorkerManager.class);

    private OplogWorkerManager(){}

    static class OplogWorkerManagerInside{
        public static final OplogWorkerManager oplogWorkerManager = new OplogWorkerManager();
    }

    public static OplogWorkerManager getInstance(){
        return OplogWorkerManagerInside.oplogWorkerManager;
    }

    private final ConcurrentHashMap<String, OplogWorker> workerMap = new ConcurrentHashMap<>(  );

    public void startWork(MongoConfig mongoConfig) throws WorkerException{
        String strWorkerId = mongoConfig.getStrWorkerId();
        OplogWorker worker = null;
        synchronized (OplogWorkerManagerInside.oplogWorkerManager) {
            if(workerMap.containsKey( strWorkerId )){
                worker = workerMap.get( strWorkerId );
                if(worker.isStarted()){
                    return ;
                }
                worker.setMongoConfig( mongoConfig );
            } else {
                worker = new OplogWorker( mongoConfig );
                workerMap.put( worker.getMongoConfig().getStrWorkerId(), worker );
            }
        }
        logger.info( String.format( "befor start task object : %s - %s" , strWorkerId, worker.toString()));
        worker.start();
        logger.info( String.format( "after start map data : %s" , workerMap.toString()));
    }

    public void stopWork(JSONObject jsonObject, String path) throws Exception{
        if(Objects.isNull( jsonObject )){
            return;
        }

        if(!jsonObject.containsKey( "strWorkerId" )){
            logger.info( String.format( "%s 停止任务信息不存在", jsonObject.toString() ) );
            return ;
        }

        String strWorkerId = jsonObject.getString( "strWorkerId" );
        OplogWorker oplogWorker = workerMap.get( strWorkerId );

        if(Objects.isNull(oplogWorker)){
            logger.info( String.format( "%s 工作任务不存在", path) );
            return ;
        }

        if(!oplogWorker.isStarted()){
            logger.info( String.format( "%s 工作任务已停止，无需再次停止", oplogWorker.getMongoConfig().toString()) );
            return ;
        }
        oplogWorker.stop();

        logger.info( String.format( "after stop map data : %s" , workerMap.toString()));
    }

    public void removeWork(JSONObject jsonObject) throws WorkerException{
        if(Objects.isNull( jsonObject )){
            return;
        }

        if(!jsonObject.containsKey( "strWorkerId" )){
            logger.info( String.format( "%s 删除任务信息不存在", jsonObject.toString() ) );
            return ;
        }
        String strWorkerId = jsonObject.getString( "strWorkerId" );
        OplogWorker oplogWorker = workerMap.get( strWorkerId );
        if(oplogWorker.isStarted()){
            logger.info( String.format( "%s 任务运行中不能删除", oplogWorker.getMongoConfig().toString() ) );
            return ;
        }

        synchronized (OplogWorkerManagerInside.oplogWorkerManager) {
            workerMap.remove( strWorkerId );
        }
        logger.info( String.format( "after remove map data : %s" , workerMap.toString()));
    }

    public void stopProcess(){
        System.exit(0);
    }


    public ConcurrentHashMap<String, OplogWorker> getWorkerMap(){
        return workerMap;
    }
}

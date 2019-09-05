package moniter;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.DBObject;
import oplog.OplogWorker;
import oplog.entity.OplogPosition;
import oplog.queue.OplogEventRingBuffer;
import oplog.task.OplogEventHandler;
import org.bson.types.BSONTimestamp;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 * 同步监控对象
 * @Author huwenhu
 * @Date 2019/7/25 11:39
 **/
public class SyncMoniter {

    private SyncMoniter(){}

    private static SyncMoniter syncMoniter = new SyncMoniter();

    public static SyncMoniter getInstance(){
        return syncMoniter;
    }

    /**
     * 获取任务状态
     * @param oplogWorker
     * @return
     */
    public JSONObject getWorkerState(OplogWorker oplogWorker){
        // 状态
        JSONObject stateJsonObject = new JSONObject();
        // 任务状态
        boolean bWorkerRunState = oplogWorker.isStarted();
        // 抓取状态
        boolean bFetchRunState = oplogWorker.getOplogFetcherTask().isRuning();
        // 目标转发状态
        JSONObject targetStateJsonObject = new JSONObject();
        ArrayList<OplogEventHandler> eventHandlerList = oplogWorker.getOplogEventHandler();
        int nEventHandleListSize = eventHandlerList.size();
        for(int j = 0;j < nEventHandleListSize;j++){
            OplogEventHandler oplogEventHandler = eventHandlerList.get(j);
            boolean bBatchEventRunState = oplogEventHandler.getOplogEventBEP().isRunning();
            targetStateJsonObject.put(oplogEventHandler.getStrSign(), bBatchEventRunState);
        }
        // 添加状态信息
        stateJsonObject.put("w", bWorkerRunState);
        stateJsonObject.put("f", bFetchRunState);
        stateJsonObject.put("t", targetStateJsonObject);
        return stateJsonObject;
    }

    /**
     * 获取任务进度
     * @param oplogWorker
     * @return
     */
    public JSONObject getWorkerRate(OplogWorker oplogWorker){
        // 状态
        JSONObject rateJsonObject = new JSONObject();
        // 日期格式化
        SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
        // 抓取位置信息
        OplogPosition fetchOplogPosition = oplogWorker.getOplogFetcherTask().getOplogPosition();
        String strFetchOplogPosition = fetchOplogPosition == null ? "" : fetchOplogPosition.toDateString(sdf);
        // 目标转发进度信息
        JSONObject targetRateJsonObject = new JSONObject();
        ArrayList<OplogEventHandler> eventHandlerList = oplogWorker.getOplogEventHandler();
        int nEventHandleListSize = eventHandlerList.size();
        for(int j = 0;j < nEventHandleListSize;j++){
            OplogEventHandler oplogEventHandler = eventHandlerList.get(j);
            DBObject successDBObject = oplogEventHandler.getOplogDispatcher().getSuccessDBObject();
            BSONTimestamp bTimeStamp = null;
            if(successDBObject != null) {
                bTimeStamp = (BSONTimestamp) successDBObject.get("ts");
            }
            StringBuilder sb = new StringBuilder();
            sb.append(sdf.format( new Date((long)bTimeStamp.getTime() * 1000L))).append("|").append(bTimeStamp.getInc());
            targetRateJsonObject.put(oplogEventHandler.getStrSign(), sb.toString());
        }
        // 添加状态信息
        rateJsonObject.put("f", strFetchOplogPosition);
        rateJsonObject.put("t", targetRateJsonObject);
        return rateJsonObject;
    }

    /**
     * 获取任务异常信息
     * @param oplogWorker
     * @return
     */
    public JSONObject getExceptionMessage(OplogWorker oplogWorker){
        // 异常
        JSONObject exceptionJsonObject = new JSONObject();
        // 启动异常
        String strWorkerExceptionMessage = oplogWorker.getExceptionMessage();
        // 抓取任务异常信息
        String strFetchExceptionMessage = oplogWorker.getOplogFetcherTask().getExceptionMessage();
        // 目标转发进度异常信息
        JSONObject targetExceptionJsonObject = new JSONObject();
        ArrayList<OplogEventHandler> eventHandlerList = oplogWorker.getOplogEventHandler();
        int nEventHandleListSize = eventHandlerList.size();
        for(int j = 0;j < nEventHandleListSize;j++){
            OplogEventHandler oplogEventHandler = eventHandlerList.get(j);
            String strTargetExceptionMessage = oplogEventHandler.getExceptionMessage();
            targetExceptionJsonObject.put(oplogEventHandler.getStrSign(), strTargetExceptionMessage);
        }
        // 添加状态信息
        exceptionJsonObject.put("w", strWorkerExceptionMessage);
        exceptionJsonObject.put("f", strFetchExceptionMessage);
        exceptionJsonObject.put("t", targetExceptionJsonObject);
        return exceptionJsonObject;
    }

    /**
     * 获取ringbuffer队列信息
     * @param oplogWorker
     * @return
     */
    public JSONObject getRingBufferData(OplogWorker oplogWorker){
        // ringbuffer队列信息
        JSONObject bufferJsonObject = new JSONObject();
        // 事件队列
        OplogEventRingBuffer oplogEventRingBuffer = oplogWorker.getOplogEventRingBuffer();

        bufferJsonObject.put("event", oplogEventRingBuffer.toJSONObject());
        return bufferJsonObject;
    }


}

package oplog.task;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import oplog.OplogDispatcher;
import oplog.entity.OplogEvent;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;


/**
 * 事件处理器
 *  1. 单生产者，多消费者模式
 *  2. 单生产者，单消费者模式
 * @Author huwenhu
 * @Date 2018/12/20 15:29
 **/
public class OplogEventHandler implements EventHandler<OplogEvent> {

    private Logger logger = Logger.getLogger(OplogEventHandler.class);
    /**
     * 任务标记封装
     */
    private String strWorkSign;
    /**
     * 当前消费者标识
     */
    private String strSign;
    /**
     * 消费者对象
     */
    private BatchEventProcessor<OplogEvent> oplogEventBEP;
    /**
     * 事件转发对象
     */
    private OplogDispatcher oplogDispatcher;
    /**
     * 异常信息
     */
    private String strExceptionMessage;
    /**
     * 是否为debug模式
     */
    private boolean isDebug;
    /**
     * 转发任务运行状态
     */
    private boolean isRun;

    public OplogEventHandler( String strWorkSign, String strSign, boolean isDebug) throws Exception {
        this.strWorkSign = strWorkSign;
        this.strSign = strSign;
        this.oplogDispatcher = new OplogDispatcher(strWorkSign);
        this.isDebug = isDebug;
    }

    /**
     * 开启转发任务
     */
    public void startDispatch() throws Exception{
        logger.info(String.format("%s-%s 事件转发任务 starting", strWorkSign, strSign));
        // 启动目标对象
        oplogDispatcher.getDMLListener().start();
        // 设置运行状态
        startSuccess();
        // 启动消费者
        new Thread(oplogEventBEP).start();
        logger.info(String.format("%s-%s 事件转发任务 started", strWorkSign, strSign));
    }

    public void onEvent(OplogEvent oplogEvent, long l, boolean b) throws Exception {

        if(!isRun){
            return ;
        }

        if(isDebug)logger.info(String.format("%s-%s 当前事件%s 队列索引 %d 是否消费完毕%b",strWorkSign, strSign,oplogEvent.toString(), l, b));


        if (oplogEvent == null) {
            logger.error(String.format("%s-%s oplogEvent is null", strWorkSign, strSign));
            return ;
        }
        setExceptionMessage("");
        try {
            // 目标对象处理任务数据
            oplogDispatcher.dispatch(oplogEvent.getDbObject());
            if(isDebug)logger.info(String.format("%s-%s 转发事件成功：%s",strWorkSign, strSign,oplogEvent.toString()));
        } catch (Exception e) {
            logger.error(String.format("%s-%s 事件转发异常：%s", strWorkSign, strSign, e.getMessage()), e);
            // 转发异常则停止消费者线程
            stopDispatch();
            setExceptionMessage(e.getMessage());
            throw new RuntimeException(String.format("%s-%s 事件转发异常：%s", strWorkSign, strSign, e.getMessage()), e);
        }
    }

    /**
     * 停止转发任务
     */
    public void stopDispatch() {
        logger.info(String.format("%s-%s 事件转发任务 stoping", strWorkSign, strSign));
        // 设置运行状态
        stopSuccess();
        // 停止消费者线程
        this.oplogEventBEP.halt();
        try {
            // 休眠500毫秒等待任务处理完毕
            TimeUnit.MILLISECONDS.sleep( 500 );
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 停止转发引擎
        this.oplogDispatcher.getDMLListener().destroy();

        logger.info(String.format("%s-%s 事件转发任务 stoped", strWorkSign, strSign));
    }

    /**
     * 获取消费者对象
     * @return
     */
    public BatchEventProcessor<OplogEvent> getOplogEventBEP(){
        return oplogEventBEP;
    }

    /**
     * 添加消费者对象
     * @param oplogEventBEP
     */
    public void setOplogEventBEP(BatchEventProcessor<OplogEvent> oplogEventBEP){
        this.oplogEventBEP = oplogEventBEP;
    }

    public OplogDispatcher getOplogDispatcher() {
        return oplogDispatcher;
    }

    public String getStrSign() {
        return strSign;
    }

    /**
     * 获取异常信息
     * @return
     */
    public String getExceptionMessage() {
        return strExceptionMessage == null ? "" : strExceptionMessage;
    }

    /**
     * 修改异常信息
     * @param strExceptionMessage
     */
    public void setExceptionMessage(String strExceptionMessage){
        this.strExceptionMessage = strExceptionMessage;
    }

    public void startSuccess(){
        isRun = true;
    }

    public void stopSuccess(){
        isRun = false;
    }
}

package oplog;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lmax.disruptor.BatchEventProcessor;
import exception.WorkerException;
import oplog.entity.OplogEvent;
import oplog.listener.DMLListener;
import oplog.queue.OplogEventRingBuffer;
import oplog.task.OplogEventHandler;
import oplog.task.OplogFetcherTask;
import org.apache.log4j.Logger;

import java.util.ArrayList;

/**
 * 工作对象
 */
public class OplogWorker {
	
	private static Logger logger = Logger.getLogger(OplogWorker.class);
	
	private MongoConfig mongoConfig;

	public OplogWorker(MongoConfig mongoConfig){
		this.mongoConfig = mongoConfig;
	}

	/**
	 * ringbuffer队列
	 */
	/**
	 * 事件队列
	 */
	private OplogEventRingBuffer oplogEventRingBuffer;
	/**
	 * 抓取线程
	 * 1. 使用Mongo配置对象创建抓取对象，连接mongo，并按照配置位置信息读取oplog日志
	 * 2. 将读取日志封装为事件对象，存入ringBuffer队列
	 */
	private OplogFetcherTask oplogFetcherTask;
	/**
	 * 事件处理器/事件消费者
	 * 1. 接收oplog日志事件，根据配置调用对应的转发器进行处理
	 * 2. 将处理成功的日志位置信息存入ringBuffer队列
	 */
	private ArrayList<OplogEventHandler> eventHandlerList;
	/**
	 * 工作状态
	 */
	private boolean isStarted = false;

	/**
	 * 异常信息
	 */
	private String strExceptionMessage;

	/**
	 * 开始工作任务
	 */
	public void start() throws WorkerException{
		try {
			if(isStarted()){
				logger.info( String.format( "%s 任务已启动" ,mongoConfig.toString() ) );
				return ;
			}
			startSuccess();
			/**
			 * ------------------
			 * 创建ringbuff队列
			 * ------------------
			 */
			// oplog事件队列
			oplogEventRingBuffer = new OplogEventRingBuffer();

			/**
			 * ----------------
			 * 创建抓取任务线程
			 * ----------------
			 */
			oplogFetcherTask = new OplogFetcherTask(mongoConfig, oplogEventRingBuffer);

			/**
			 * ------------------
			 * 创建处理器
			 * ------------------
			 */
			// oplog事件处理对象
			eventHandlerList = createOplogEventHandlerList(mongoConfig, oplogEventRingBuffer);

			/**
			 * 启动工作线程
			 */
			oplogFetcherTask.startFetch();
			startOplogEventHandler(eventHandlerList);

			setExceptionMessage("");
		} catch(Exception e){
			logger.error(mongoConfig.toString() + " 任务启动失败 " + e.getMessage(), e);
			setExceptionMessage(e.getMessage());
			// 任务启动异常，停止工作任务
			try {
				stop();
			} catch(Exception ex1){
				logger.error(mongoConfig.toString() + " 任务停止失败 " + ex1.getMessage(), ex1);
			}
			throw new WorkerException(e);
		}
	}
	
	public void stop() throws Exception{
		if(oplogFetcherTask != null){
			// 抓取线程存在则停止
			oplogFetcherTask.stopFetch();
		}
		if(eventHandlerList != null){
			// 停止事件转发器及对应的目标连接
			stopOplogEventHandler(eventHandlerList);
		}

		stopSuccess();
	}
	
	public boolean isStarted(){
		return isStarted;
	}

	public MongoConfig getMongoConfig() {
		return mongoConfig;
	}

	public OplogFetcherTask getOplogFetcherTask() {
		return oplogFetcherTask;
	}

	public ArrayList<OplogEventHandler> getOplogEventHandler() {
		return eventHandlerList;
	}

	public OplogEventRingBuffer getOplogEventRingBuffer() {
		return oplogEventRingBuffer;
	}

	/**
	 * 创建事件处理器集合
	 * @param mongoConfig				配置信息
	 * @param oplogEventRingBuffer		oplog事件队列
	 * @return
	 * @throws Exception
	 */
	public ArrayList<OplogEventHandler> createOplogEventHandlerList(MongoConfig mongoConfig, OplogEventRingBuffer oplogEventRingBuffer) throws Exception {

		JSONArray strDMLTargetJSONArray = mongoConfig.getStrDMLTargetJSONArray();

		// 获取转发目标配置数量
		int nTargetJSON = strDMLTargetJSONArray.size();
		if(nTargetJSON == 0){
			throw new RuntimeException("转发目标配置信息不存在");
		}
		// 创建多事件处理器
		ArrayList<OplogEventHandler> eventHandlerList = new ArrayList<>();
		for(int i = 0;i < nTargetJSON;i++){
			JSONObject jsonObject = strDMLTargetJSONArray.getJSONObject(i);
			// 获取消费者编号
			String strSign = jsonObject.getString("strSign");
			// 创建处理器对象
			OplogEventHandler oplogEventHandler = new OplogEventHandler(mongoConfig.toString(), strSign, mongoConfig.isDebug());

			// 创建消费者对象
			BatchEventProcessor<OplogEvent> oplogEventBEP = oplogEventRingBuffer.createConsumer(oplogEventHandler);
			// 创建目标对象
			String strDMLTargetClass = jsonObject.getString("strDMLTargetClass");
			Class classTarget = Class.forName(strDMLTargetClass);
			DMLListener dmlListener = (DMLListener)classTarget.newInstance();
			if(dmlListener == null){
				throw new RuntimeException(String.format("%s-%s 目标类%s不存在",mongoConfig.toString(), strSign, strDMLTargetClass));
			}
			dmlListener.init(mongoConfig.toString(), strSign, jsonObject, mongoConfig.isDebug());
			// 设置消费者和目标对象
			oplogEventHandler.setOplogEventBEP(oplogEventBEP);
			oplogEventHandler.getOplogDispatcher().setDMLListener(dmlListener);
			eventHandlerList.add(oplogEventHandler);
		}
		return eventHandlerList;
	}

	/**
	 * 启动事件处理器
	 * @param eventHandlerList
	 * @throws Exception
	 */
	public void startOplogEventHandler(ArrayList<OplogEventHandler> eventHandlerList) throws Exception {
		if(eventHandlerList == null){
			throw new RuntimeException("处理器集合 is null");
		}
		// 启动事件处理器
		int nEventHandlerList = eventHandlerList.size();
		for(int i = 0;i < nEventHandlerList;i++){
			OplogEventHandler oplogEventHandler = eventHandlerList.get(i);
			oplogEventHandler.startDispatch();
		}
	}

	/**
	 * 停止事件处理器
	 * @param eventHandlerList
	 * @throws Exception
	 */
	public void stopOplogEventHandler(ArrayList<OplogEventHandler> eventHandlerList) throws Exception {
		if(eventHandlerList == null){
			throw new RuntimeException("处理器集合 is null");
		}
		// 停止事件处理器
		int nEventHandlerList = eventHandlerList.size();
		for(int i = 0;i < nEventHandlerList;i++){
			OplogEventHandler oplogEventHandler = eventHandlerList.get(i);
			oplogEventHandler.stopDispatch();
		}
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
		isStarted = true;
	}

	public void stopSuccess(){
		isStarted = false;
	}

	public void setMongoConfig(MongoConfig mongoConfig){
		this.mongoConfig = mongoConfig;
	}

}

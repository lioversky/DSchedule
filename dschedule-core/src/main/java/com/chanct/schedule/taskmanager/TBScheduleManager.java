package com.chanct.schedule.taskmanager;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chanct.schedule.bean.IScheduleTaskDeal;
import com.chanct.schedule.datamanager.IScheduleDataManager;
import com.chanct.schedule.entity.StatisticsInfo;
import com.chanct.schedule.entity.TaskItemDefine;
import com.chanct.schedule.entity.manager.ScheduleServer;
import com.chanct.schedule.entity.manager.ScheduleTaskType;
import com.chanct.schedule.entity.strategy.ScheduleStrategy;
import com.chanct.schedule.processor.IScheduleProcessor;
import com.chanct.schedule.processor.TBScheduleProcessorGroup;
import com.chanct.schedule.processor.TBScheduleProcessorNormal;
import com.chanct.schedule.processor.TBScheduleProcessorNotSleep;
import com.chanct.schedule.processor.TBScheduleProcessorSleep;
import com.chanct.schedule.stragety.IStrategyTask;
import com.chanct.schedule.stragety.TBScheduleFactory;
import com.chanct.schedule.taskmanager.task.PauseOrResumeScheduleTask;
import com.chanct.schedule.util.CronExpression;
import com.chanct.schedule.util.ScheduleUtil;




/**
 * 1、任务调度分配器的目标：	让所有的任务不重复，不遗漏的被快速处理。
 * 2、一个Manager只管理一种任务类型的一组工作线程。
 * 3、在一个JVM里面可能存在多个处理相同任务类型的Manager，也可能存在处理不同任务类型的Manager。
 * 4、在不同的JVM里面可以存在处理相同任务的Manager 
 * 5、调度的Manager可以动态的随意增加和停止
 * 
 * 主要的职责：
 * 1、定时向集中的数据配置中心更新当前调度服务器的心跳状态
 * 2、向数据配置中心获取所有服务器的状态来重新计算任务的分配。这么做的目标是避免集中任务调度中心的单点问题。
 * 3、在每个批次数据处理完毕后，检查是否有其它处理服务器申请自己把持的任务队列，如果有，则释放给相关处理服务器。
 *  
 * 其它：
 * 	 如果当前服务器在处理当前任务的时候超时，需要清除当前队列，并释放已经把持的任务。并向控制主动中心报警。
 * 
 * @author xuannan
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class TBScheduleManager implements IStrategyTask {
	private static transient Logger log = LoggerFactory.getLogger(TBScheduleManager.class);
	/**
	 * 用户标识不同线程的序号
	 */
	private static int nextSerialNumber = 0;
 
	/**
	 * 当前线程组编号
	 */
	protected int currentSerialNumber=0;
	/**
	 * 调度任务类型信息
	 */
	protected ScheduleTaskType taskTypeInfo;
	/**
	 * 当前调度服务的信息
	 */
	protected ScheduleServer currenScheduleServer;
	/**
	 * 队列处理器
	 */
	IScheduleTaskDeal  taskDealBean;
	
    /**
     * 多线程任务处理器
     */
	IScheduleProcessor processor;
	ScheduleStrategy strategy ;
    StatisticsInfo statisticsInfo = new StatisticsInfo();
    
    boolean isPauseSchedule = true;
    String pauseMessage="";
    /**
     *  当前处理任务队列清单
     */
    protected List<TaskItemDefine> currentTaskItemList = new ArrayList<TaskItemDefine>();
    /**
     * 最近一起重新装载调度任务的时间。
     * 当前实际  - 上此装载时间  > intervalReloadTaskItemList，则向配置中心请求最新的任务分配情况
     */
    protected long lastReloadTaskItemListTime=0;    
    protected boolean isNeedReloadTaskItem = true;

    
    private String mBeanName;
    /**
     * 向配置中心更新信息的定时器
     */
    protected Timer heartBeatTimer;

    protected  IScheduleDataManager scheduleCenter;
    
    public IScheduleDataManager getScheduleCenter() {
		return scheduleCenter;
	}
	public void setScheduleCenter(IScheduleDataManager scheduleCenter) {
		this.scheduleCenter = scheduleCenter;
	}
	protected String startErrorInfo = null;
    
    protected boolean isStopSchedule = false;
    protected Lock registerLock = new ReentrantLock();
    
    /**
     * 运行期信息是否初始化成功
     */
    protected boolean isRuntimeInfoInitial = false;
    
    TBScheduleFactory factory;
    protected String ownSign ;
    protected String baseTaskType;
	TBScheduleManager(TBScheduleFactory aFactory, ScheduleStrategy strategy ,IScheduleDataManager scheduleDataManager) throws Exception{
		this.factory = aFactory;
		this.strategy = strategy;
		this.currentSerialNumber = serialNumber();
		this.scheduleCenter = scheduleDataManager;
		this.baseTaskType = strategy.getTaskName();
		
//		this.mBeanName = "pamirs:name=" + "schedule.ServerMananger." +this.currenScheduleServer.getUuid();
		this.ownSign = strategy.getOwnSign();
		/*
		  this.taskTypeInfo = this.scheduleCenter.loadTaskTypeBaseInfo(baseTaskType);
		//清除已经过期1天的TASK,OWN_SIGN的组合。超过一天没有活动server的视为过期
				this.scheduleCenter.clearExpireTaskTypeRunningInfo(baseTaskType,ScheduleUtil.getLocalIP() + "清除过期OWN_SIGN信息",this.taskTypeInfo.getExpireOwnSignInterval());
//				获取执行的bean
				Object dealBean = aFactory.getBean(this.taskTypeInfo.getDealBeanName());
				if (dealBean == null) {
					throw new Exception( "SpringBean " + this.taskTypeInfo.getDealBeanName() + " 不存在");
				}
				if (dealBean instanceof IScheduleTaskDeal == false) {
					throw new Exception( "SpringBean " + this.taskTypeInfo.getDealBeanName() + " 没有实现 IScheduleTaskDeal接口");
				}
		    	this.taskDealBean = (IScheduleTaskDeal)dealBean;

		    	if(this.taskTypeInfo.getJudgeDeadInterval() < this.taskTypeInfo.getHeartBeatRate() * 5){
		    		throw new Exception("数据配置存在问题，死亡的时间间隔，至少要大于心跳线程的5倍。当前配置数据：JudgeDeadInterval = "
		    				+ this.taskTypeInfo.getJudgeDeadInterval() 
		    				+ ",HeartBeatRate = " + this.taskTypeInfo.getHeartBeatRate());
		    	}
//		    	初始化ScheduleServer
		    	this.currenScheduleServer = ScheduleServer.createScheduleServer(this.scheduleCenter,baseTaskType,ownSign,this.taskTypeInfo.getThreadNumber());
		    	this.currenScheduleServer.setManagerFactoryUUID(this.factory.getUuid());
//		    	注册当前的处理服务器
		    	scheduleCenter.registerScheduleServer(this.currenScheduleServer);
		    	
				this.heartBeatTimer = new Timer(this.currenScheduleServer.getTaskType() + "-" + this.currentSerialNumber + "-HeartBeat");
//				注册timer，定时向数据配置中心更新当前服务器的心跳信息
				this.heartBeatTimer.schedule(new HeartBeatTimerTask(this), new java.util.Date(System.currentTimeMillis() + 500), this.taskTypeInfo.getHeartBeatRate());
				*/
	}  
	/**
	 * 对象创建时需要做的初始化工作
	 * 
	 * @throws Exception
	 */
	public abstract void initial() throws Exception;
	public abstract void refreshScheduleServerInfo() throws Exception;
	public abstract void assignScheduleTask() throws Exception;
	public abstract List<TaskItemDefine> getCurrentScheduleTaskItemList();
	public abstract int getTaskItemCount();
	public String getTaskType(){
		return this.currenScheduleServer.getTaskType();
	}
	
	public void initialTaskParameter(String strategyName,String taskParameter){
	   //没有实现的方法，需要的参数直接从任务配置中读取	
	}
	private static synchronized int serialNumber() {
	        return nextSerialNumber++;
	}	

	public int getCurrentSerialNumber(){
		return this.currentSerialNumber;
	}
	/**
	 * 清除内存中所有的已经取得的数据和任务队列,在心态更新失败，或者发现注册中心的调度信息被删除
	 */
	public void clearMemoInfo(){
		try {
			// 清除内存中所有的已经取得的数据和任务队列,在心态更新失败，或者发现注册中心的调度信息被删除
			this.currentTaskItemList.clear();
			if (this.processor != null) {
				this.processor.clearAllHasFetchData();
			}
		} finally {
			//设置内存里面的任务数据需要重新装载
			this.isNeedReloadTaskItem = true;
		}

	}
	
	public void rewriteScheduleInfo() throws Exception{
		registerLock.lock();
		try{
			if (this.isStopSchedule == true) {
				if(log.isDebugEnabled()){
					log.debug("外部命令终止调度,不在注册调度服务，避免遗留垃圾数据：" + currenScheduleServer.getUuid());
				}
				return;
			}
		//先发送心跳信息
		if(startErrorInfo == null){
			this.currenScheduleServer.setDealInfoDesc(this.pauseMessage + ":" + this.statisticsInfo.getDealDescription());
		}else{
		    this.currenScheduleServer.setDealInfoDesc(startErrorInfo);
		}
		if(	this.scheduleCenter.refreshScheduleServer(this.currenScheduleServer) == false){
			//更新信息失败，清除内存数据后重新注册
			this.clearMemoInfo();
			this.scheduleCenter.registerScheduleServer(this.currenScheduleServer);
		}
		}finally{
			registerLock.unlock();
		}
	}

	


	/**
	 * 开始的时候，计算第一次执行时间
	 * @throws Exception
	 */
    public void computerStart() throws Exception{
    	//只有当存在可执行队列后再开始启动队列
   	
    	boolean isRunNow = false;
    	if(this.strategy.getExecuteType()==ScheduleStrategy.Type.StartRunEveryTime){
//    	}
//    	if(this.strategy.getPermitRunStartTime() == null){
    		isRunNow = true;
		} else {
			String tmpStr = this.strategy.getPermitRunStartTime();
			tmpStr = this.strategy.getExecuteStrategy();
			CronExpression cexpStart = new CronExpression(tmpStr);
			Date current = new Date(System.currentTimeMillis());
			Date firstStartTime = cexpStart.getNextValidTimeAfter(current);
			this.heartBeatTimer.schedule(new PauseOrResumeScheduleTask(this, this.heartBeatTimer, PauseOrResumeScheduleTask.TYPE_RESUME, tmpStr), firstStartTime);
			this.currenScheduleServer.setNextRunStartTime(ScheduleUtil.transferDataToString(firstStartTime));
			// 没有结束时间
			if (this.strategy.getPermitRunEndTime() == null || this.strategy.getPermitRunEndTime().equals("-1")) {
				this.currenScheduleServer.setNextRunEndTime("当不能获取到数据的时候pause");
			} else {
				// 创建结束任务守护线程
				try {
					String tmpEndStr = this.strategy.getPermitRunEndTime();
					CronExpression cexpEnd = new CronExpression(tmpEndStr);
					Date firstEndTime = cexpEnd.getNextValidTimeAfter(firstStartTime);
					Date nowEndTime = cexpEnd.getNextValidTimeAfter(current);
					if (!nowEndTime.equals(firstEndTime) && current.before(nowEndTime)) {
						// isRunNow = true;
						firstEndTime = nowEndTime;
					}
					this.heartBeatTimer.schedule(new PauseOrResumeScheduleTask(this, this.heartBeatTimer, PauseOrResumeScheduleTask.TYPE_PAUSE, tmpEndStr), firstEndTime);
					this.currenScheduleServer.setNextRunEndTime(ScheduleUtil.transferDataToString(firstEndTime));
				} catch (Exception e) {
					log.error("计算第一次执行时间出现异常:" + currenScheduleServer.getUuid(), e);
					throw new Exception("计算第一次执行时间出现异常:" + currenScheduleServer.getUuid(), e);
				}
			}
		}
    	if(isRunNow == true){
    		this.resume("开启服务立即启动");
    	}
    	this.rewriteScheduleInfo();
    	
    }
	/**
	 * 当Process没有获取到数据的时候调用，决定是否暂时停止服务器
	 * @throws Exception
	 */
	public boolean isContinueWhenData() throws Exception{
		if(isPauseWhenNoData() == true){
			this.pause("没有数据,暂停调度");
			return false;
		}else{
			return true;
		}
	}
	public boolean isPauseWhenNoData(){
		//如果还没有分配到任务队列则不能退出
		if(this.currentTaskItemList.size() >0 && this.strategy.getPermitRunStartTime() != null){
			if(this.strategy.getPermitRunEndTime() == null
		       || this.strategy.getPermitRunEndTime().equals("-1")){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
	}	
	/**
	 * 超过运行的运行时间，暂时停止调度
	 * @throws Exception 
	 */
	public void pause(String message) throws Exception{
		if (this.isPauseSchedule == false) {
			this.isPauseSchedule = true;
			this.pauseMessage = message;
			if (log.isDebugEnabled()) {
				log.debug("暂停调度 ：" + this.currenScheduleServer.getUuid()+":" + this.statisticsInfo.getDealDescription());
			}
			if (this.processor != null) {
				this.processor.stopSchedule();
			}
			rewriteScheduleInfo();
		}
	}
	/**
	 * 处在了可执行的时间区间，恢复运行
	 * @throws Exception 
	 */
	public void resume(String message) throws Exception {
		if (this.isPauseSchedule == true) {
			if (log.isDebugEnabled()) {
				log.debug("恢复调度:" + this.currenScheduleServer.getUuid());
			}
			
			this.pauseMessage = message;
			if (this.taskDealBean != null) {
				if (this.taskTypeInfo.getKind() ==ScheduleTaskType.Kind.NotSleep) {
					this.processor = new TBScheduleProcessorNotSleep(this, taskDealBean, this.statisticsInfo);
					this.isPauseSchedule = false;
				} else if(this.taskTypeInfo.getKind() ==ScheduleTaskType.Kind.Sleep){
					this.processor = new TBScheduleProcessorSleep(this, taskDealBean, this.statisticsInfo);
					this.isPauseSchedule = false;
				} else if(this.taskTypeInfo.getKind() ==ScheduleTaskType.Kind.Normal){
					this.processor = new TBScheduleProcessorNormal(this, taskDealBean, this.statisticsInfo);
					this.isPauseSchedule = true;
				}else if(this.taskTypeInfo.getKind() ==ScheduleTaskType.Kind.Group){
					this.processor = new TBScheduleProcessorGroup(this, taskDealBean, this.statisticsInfo,this.factory.getScheduleRuntimeManager());
					this.isPauseSchedule = true;
				}
			}
			rewriteScheduleInfo();
		}
	}
	/**
	 * 当服务器停止的时候，调用此方法清除所有未处理任务，清除服务器的注册信息。
	 * 也可能是控制中心发起的终止指令。
	 * 需要注意的是，这个方法必须在当前任务处理完毕后才能执行
	 * @throws Exception 
	 */
	public void stop(String strategyName) throws Exception{
		if(log.isDebugEnabled()){
			log.debug("停止服务器 ：" + this.currenScheduleServer.getUuid());
		}
		this.isPauseSchedule = false;
		if (this.processor != null) {
			this.processor.stopSchedule();
		} else {
			this.unRegisterScheduleServer();
		}
	}
	
	/**
	 * 只应该在Processor中调用
	 * @throws Exception
	 */
	public void unRegisterScheduleServer() throws Exception{
		System.out.println(11111);
		registerLock.lock();
		try {
			if (this.processor != null) {
				this.processor = null;
			}
			if (this.isPauseSchedule == true) {
				// 是暂停调度，不注销Manager自己
				return;
			}
			if (log.isDebugEnabled()) {
				log.debug("注销服务器 ：" + this.currenScheduleServer.getUuid());
			}
			this.isStopSchedule = true;
			// 取消心跳TIMER
			this.heartBeatTimer.cancel();
			// 从配置中心注销自己
			this.scheduleCenter.unRegisterScheduleServer(
					this.currenScheduleServer.getTaskType(),
					this.currenScheduleServer.getUuid());
		} finally {
			registerLock.unlock();
		}
	}
	public ScheduleTaskType getTaskTypeInfo() {
		return taskTypeInfo;
	}	
	
	
	public StatisticsInfo getStatisticsInfo() {
		return statisticsInfo;
	}
	/**
	 * 打印给定任务类型的任务分配情况
	 * @param taskType
	 */
	public void printScheduleServerInfo(String taskType){
		
	}
	public ScheduleServer getScheduleServer(){
		return this.currenScheduleServer;
	}
	public String getmBeanName() {
		return mBeanName;
	}
	public IScheduleProcessor getProcessor() {
		return processor;
	}
	public void setProcessor(IScheduleProcessor processor) {
		this.processor = processor;
	}
	
}





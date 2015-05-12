package com.chanct.schedule.processor;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chanct.schedule.bean.IScheduleTaskDeal;
import com.chanct.schedule.bean.IScheduleTaskDealSingle;
import com.chanct.schedule.entity.StatisticsInfo;
import com.chanct.schedule.entity.manager.ScheduleTaskType;
import com.chanct.schedule.taskmanager.TBScheduleManager;
import com.chanct.schedule.util.ScheduleUtil;

/**
 * 简单的任务
 * 
 * @author : lihx create date : 2014-11-17
 */
public class TBScheduleProcessorNormal<T> implements IScheduleProcessor, Runnable {
	private static transient Logger logger = LoggerFactory.getLogger(TBScheduleProcessorNormal.class);
	/**
	 * 任务管理器
	 */
	protected TBScheduleManager scheduleManager;
	/**
	 * 任务类型
	 */
	ScheduleTaskType taskTypeInfo;
	/**
	 * 任务处理的接口类
	 */
	protected IScheduleTaskDeal<T> taskDealBean;

	StatisticsInfo statisticsInfo;

	private boolean isStopSchedule;
	String date_time = "";
	
	public TBScheduleProcessorNormal(TBScheduleManager aManager, IScheduleTaskDeal<T> aTaskDealBean, StatisticsInfo aStatisticsInfo) {
		this.scheduleManager = aManager;
		this.statisticsInfo = aStatisticsInfo;
		this.taskTypeInfo = this.scheduleManager.getTaskTypeInfo();
		this.taskDealBean = aTaskDealBean;
		this.date_time = ScheduleUtil.transferDataToString(new Date(), "yyyyMMddHHmmss");
		Thread thread = new Thread(this);
		String threadName = this.scheduleManager.getScheduleServer().getTaskType() + "-" + this.scheduleManager.getCurrentSerialNumber() + "-exe";
		thread.setName(threadName);
		thread.start();
	}
	@SuppressWarnings({"rawtypes", "unchecked"})
	@Override
	public void run() {
		try {
			logger.info("Run job "+taskDealBean.getClass().getName());
			((IScheduleTaskDealSingle) this.taskDealBean).execute(taskTypeInfo.getTaskParameter(), date_time);
			this.scheduleManager.setProcessor(null);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (isStopSchedule) {
				try {
					this.scheduleManager.unRegisterScheduleServer();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public boolean isDealFinishAllData() {
		return true ;
	}

	@Override
	public boolean isSleeping() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void stopSchedule() throws Exception {
		
		this.isStopSchedule = true;
	}

	@Override
	public void clearAllHasFetchData() {
		
	}

}

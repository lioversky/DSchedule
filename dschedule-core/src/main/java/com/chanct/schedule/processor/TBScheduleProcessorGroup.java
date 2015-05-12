package com.chanct.schedule.processor;

import java.util.Date;
import java.util.Map;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chanct.schedule.bean.IScheduleTaskDeal;
import com.chanct.schedule.bean.IScheduleTaskDealSingle;
import com.chanct.schedule.datamanager.IScheduleRuntimeManager;
import com.chanct.schedule.entity.StatisticsInfo;
import com.chanct.schedule.entity.manager.ScheduleTaskItem;
import com.chanct.schedule.entity.manager.ScheduleTaskType;
import com.chanct.schedule.entity.manager.ScheduleTaskTypeRunningInfo;
import com.chanct.schedule.taskmanager.TBScheduleManager;
import com.chanct.schedule.util.ScheduleUtil;

/**
 * 基于任务组类型的任务 任务启动后，并不先执行，而是根据自身的任务序号，启动挨着自己小一号的任务监听。 如果本序号是0，则立即执行，否则当监听的任务执行状态为成功时再执行
 * 当所监听的任务大于1时，表示为合并的任务 每次任务组启动都有执行序号*****
 * 
 * 
 * 问题： 1.根据启动产生的参数传递； 2.出错宕机时如何找到序号继续执行； 3.zookeeper中不能有太多数据； 4.序号由哪个任务负责生成，生成后其它任务怎么获取；
 * yyyyMMddhh:mi:ss_seq
 * 
 * 
 * 解决：
 * 
 * 创建此类时先构建包括所有任务的执行结构和当前时间的字符串
 * 1.由第一个任务负责在taskitem下生成yyyyMMddhhmiss_seq节点，其它节点监听产生事件后，取当前最大序列号后，并再监听比本身任务号小的任务状态
 * 2.最后一个任务在任务组结束时删除yyyyMMddhhmiss_seq
 * 
 * 
 * 
 * 
 * @author : lihx create date : 2014-11-17
 */
public class TBScheduleProcessorGroup<T> implements IScheduleProcessor, Runnable {
	
	private static transient Logger logger = LoggerFactory.getLogger(TBScheduleProcessorGroup.class);
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

	private Object obj_lock = new Object();
	IScheduleRuntimeManager runtimeManager;
	String date_time = "";
//	前一个任务
	String previousTask = "";
//	下一个任务
	String nextTask = "";
	public TBScheduleProcessorGroup(TBScheduleManager aManager, IScheduleTaskDeal<T> aTaskDealBean, StatisticsInfo aStatisticsInfo, IScheduleRuntimeManager runtimeManager) throws Exception {
		this.scheduleManager = aManager;
		this.statisticsInfo = aStatisticsInfo;
		this.taskTypeInfo = this.scheduleManager.getTaskTypeInfo();
		this.taskDealBean = aTaskDealBean;
		this.date_time = ScheduleUtil.transferDataToString(new Date(), "yyyyMMddHHmmss");
		this.runtimeManager = runtimeManager;
		Map<String,Integer> groups = scheduleManager.getScheduleCenter().loadGroupMapInfo(taskTypeInfo.getBaseTaskType());
		previousTask = previousTask(taskTypeInfo.getGroupItemName(), taskTypeInfo.getGroupIndex(), groups);
		nextTask = nextTask(taskTypeInfo.getGroupItemName(), taskTypeInfo.getGroupIndex(), groups);
		// this.scheduleManager.getScheduleCenter().watchItem(taskTypeInfo, watcher)
		Thread thread = new Thread(this);
		String threadName = this.scheduleManager.getScheduleServer().getTaskType() + "-" + this.scheduleManager.getCurrentSerialNumber() + "-group-exe";
		thread.setName(threadName);
		thread.start();
	}
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	@Override
	public void run() {
		try {
			if (taskTypeInfo.getGroupIndex() == 1) {
				runtimeManager.createRunTimeSts(this.scheduleManager.getScheduleServer().getTaskType(), taskTypeInfo.getGroupItemName(), date_time);
			}else {
				synchronized(obj_lock){
				// 不是第一个节点，监听组内前一个任务状态
					if (runtimeManager.watchPreviousItem(this.scheduleManager.getScheduleServer().getTaskType(), previousTask, date_time, new GroupWatcher(this), taskTypeInfo.getGroupIndex())) {
//						System.out.println(date_time+":"+ taskTypeInfo.getGroupItemName()+"wait-----");
						obj_lock.wait();
					}
					runtimeManager.createRunTimeSts(this.scheduleManager.getScheduleServer().getTaskType(), taskTypeInfo.getGroupItemName(), date_time);
				}
			}
			logger.info("Run job "+taskTypeInfo.getClass().getName());
			((IScheduleTaskDealSingle) this.taskDealBean).execute(taskTypeInfo.getTaskParameter(),date_time);
			this.scheduleManager.setProcessor(null);
			{
				// 执行完成后，修改runtime状态为结束
				runtimeManager.finishRunTimeItem(this.scheduleManager.getScheduleServer().getTaskType(), taskTypeInfo.getGroupItemName(), date_time);
				if(nextTask==null) {
//					最后 一个任务，清除执行节点
					runtimeManager.deleteFinishNode(this.scheduleManager.getScheduleServer().getTaskType(),  date_time);
					logger.warn("任务："+this.scheduleManager.getScheduleServer().getTaskType()+"的子任务："+taskTypeInfo.getGroupItemName()+"参数："+date_time+"执行完成，清除执行信息！");
					
				}
			}
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

	private String previousTask(String taskName, int index, Map<String, Integer> groups) {
		String previousTask = null;
		for (Map.Entry<String, Integer> entry : groups.entrySet()) {
			if (entry.getValue() == index - 1) {
				previousTask = entry.getKey();
				break;
			}
		}
		return previousTask;
	}
	private String nextTask(String taskName , int index,Map<String,Integer> groups) {
		String nextTask = null;
		for (Map.Entry<String, Integer> entry : groups.entrySet()) {
			if (entry.getValue() == index + 1) {
				nextTask = entry.getKey();
				break;
			}
		}
		return nextTask;
	}
	@Override
	public boolean isDealFinishAllData() {
		return true;
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

	public void notifyAllLock() {
		synchronized (obj_lock) {
			obj_lock.notifyAll();
		}
	}

}
class GroupWatcher implements CuratorWatcher {

	TBScheduleProcessorGroup processor = null;
	private String path = null;
	public GroupWatcher(TBScheduleProcessorGroup processor) {
		this.processor = processor;
	}
	@Override
	public void process(WatchedEvent event) {
		try {
			path = event.getPath();
			String taskType = processor.scheduleManager.getScheduleServer().getTaskType();
			String time = processor.date_time;
			ScheduleTaskTypeRunningInfo info = processor.runtimeManager.loadRunningInfo(path);
			// 判断所监听的节点状态为finish，唤醒所有此锁下的线程。否则继续监听
			if (ScheduleTaskItem.TaskItemSts.FINISH.equals(info.getSts())) {
				processor.notifyAllLock();
			} else {
				if (!processor.runtimeManager.watchPreviousItem(taskType, processor.previousTask, time, this, processor.taskTypeInfo.getGroupIndex())) {
					processor.notifyAllLock();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}

}
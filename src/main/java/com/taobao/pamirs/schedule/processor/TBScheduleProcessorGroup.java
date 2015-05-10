package com.taobao.pamirs.schedule.processor;

import java.util.Date;
import java.util.Map;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.bean.IScheduleTaskDeal;
import com.taobao.pamirs.schedule.bean.IScheduleTaskDealSingle;
import com.taobao.pamirs.schedule.datamanager.IScheduleRuntimeManager;
import com.taobao.pamirs.schedule.entity.StatisticsInfo;
import com.taobao.pamirs.schedule.entity.manager.ScheduleTaskItem;
import com.taobao.pamirs.schedule.entity.manager.ScheduleTaskType;
import com.taobao.pamirs.schedule.entity.manager.ScheduleTaskTypeRunningInfo;
import com.taobao.pamirs.schedule.taskmanager.TBScheduleManager;
import com.taobao.pamirs.schedule.util.ScheduleUtil;

/**
 * �������������͵����� ���������󣬲�����ִ�У����Ǹ��������������ţ����������Լ�Сһ�ŵ���������� ����������0��������ִ�У����򵱼���������ִ��״̬Ϊ�ɹ�ʱ��ִ��
 * �����������������1ʱ����ʾΪ�ϲ������� ÿ����������������ִ�����*****
 * 
 * 
 * ���⣺ 1.�������������Ĳ������ݣ� 2.����崻�ʱ����ҵ���ż���ִ�У� 3.zookeeper�в�����̫�����ݣ� 4.������ĸ����������ɣ����ɺ�����������ô��ȡ��
 * yyyyMMddhh:mi:ss_seq
 * 
 * 
 * �����
 * 
 * ��������ʱ�ȹ����������������ִ�нṹ�͵�ǰʱ����ַ���
 * 1.�ɵ�һ����������taskitem������yyyyMMddhhmiss_seq�ڵ㣬�����ڵ���������¼���ȡ��ǰ������кź󣬲��ټ����ȱ��������С������״̬
 * 2.���һ�����������������ʱɾ��yyyyMMddhhmiss_seq
 * 
 * 
 * 
 * 
 * @author : lihx create date : 2014-11-17
 */
public class TBScheduleProcessorGroup<T> implements IScheduleProcessor, Runnable {
	
	private static transient Logger logger = LoggerFactory.getLogger(TBScheduleProcessorGroup.class);
	/**
	 * ���������
	 */
	protected TBScheduleManager scheduleManager;
	/**
	 * ��������
	 */
	ScheduleTaskType taskTypeInfo;
	/**
	 * ������Ľӿ���
	 */
	protected IScheduleTaskDeal<T> taskDealBean;

	StatisticsInfo statisticsInfo;

	private boolean isStopSchedule;

	private Object obj_lock = new Object();
	IScheduleRuntimeManager runtimeManager;
	String date_time = "";
//	ǰһ������
	String previousTask = "";
//	��һ������
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
				// ���ǵ�һ���ڵ㣬��������ǰһ������״̬
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
				// ִ����ɺ��޸�runtime״̬Ϊ����
				runtimeManager.finishRunTimeItem(this.scheduleManager.getScheduleServer().getTaskType(), taskTypeInfo.getGroupItemName(), date_time);
				if(nextTask==null) {
//					��� һ���������ִ�нڵ�
					runtimeManager.deleteFinishNode(this.scheduleManager.getScheduleServer().getTaskType(),  date_time);
					logger.warn("����"+this.scheduleManager.getScheduleServer().getTaskType()+"��������"+taskTypeInfo.getGroupItemName()+"������"+date_time+"ִ����ɣ����ִ����Ϣ��");
					
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
			// �ж��������Ľڵ�״̬Ϊfinish���������д����µ��̡߳������������
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
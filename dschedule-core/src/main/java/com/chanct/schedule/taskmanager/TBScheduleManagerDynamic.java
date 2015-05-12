package com.chanct.schedule.taskmanager;

import java.util.List;

import com.chanct.schedule.datamanager.IScheduleDataManager;
import com.chanct.schedule.entity.TaskItemDefine;
import com.chanct.schedule.stragety.TBScheduleFactory;

public class TBScheduleManagerDynamic extends TBScheduleManager {
	//private static transient Log log = LogFactory.getLog(TBScheduleManagerDynamic.class);
	
	TBScheduleManagerDynamic(TBScheduleFactory aFactory, String baseTaskType, String ownSign, int managerPort, String jxmUrl, IScheduleDataManager aScheduleCenter) throws Exception {
		super(aFactory, null, aScheduleCenter);
	}
	public void initial() throws Exception{
		if (scheduleCenter.isLeader(this.currenScheduleServer.getUuid(),
				scheduleCenter.loadScheduleServerNames(this.currenScheduleServer.getTaskType()))) {
			// 是第一次启动，检查对应的zk目录是否存在
			this.scheduleCenter.initialRunningInfo4Dynamic(	this.currenScheduleServer.getBaseTaskType(),
					this.currenScheduleServer.getOwnSign());
		}
		computerStart();
    }
	
	public void refreshScheduleServerInfo() throws Exception {
		throw new Exception("没有实现");
	}

	public boolean isNeedReLoadTaskItemList() throws Exception {
		throw new Exception("没有实现");
	}
	public void assignScheduleTask() throws Exception {
		throw new Exception("没有实现");
		
	}
	public List<TaskItemDefine> getCurrentScheduleTaskItemList() {
		throw new RuntimeException("没有实现");
	}
	public int getTaskItemCount() {
		throw new RuntimeException("没有实现");
	}
}

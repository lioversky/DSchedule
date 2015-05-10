package com.taobao.pamirs.schedule.strategy.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.strategy.TBSchedule;

/**
 * 
 * 
 * @author : lihx create date : 2014-11-18
 */
public class AssignFactoryTimerTask extends java.util.TimerTask {
	private static transient Logger log = LoggerFactory.getLogger(AssignFactoryTimerTask.class);
	TBSchedule manager;

	public AssignFactoryTimerTask(TBSchedule manager) {
		this.manager = manager;
	}

	public void run() {
		try {
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			this.manager.assignScheduleFactory();
		} catch (Throwable ex) {
			log.error(ex.getMessage(), ex);
		}
	}
}

package com.taobao.pamirs.schedule.taskmanager.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.taskmanager.TBScheduleManager;

/**
 *
 *
 * @author : lihx
 * create date : 2014-11-18
 */
public class HeartBeatTimerTask extends java.util.TimerTask {
	private static transient Logger log = LoggerFactory
			.getLogger(HeartBeatTimerTask.class);
	TBScheduleManager manager;

	public HeartBeatTimerTask(TBScheduleManager aManager) {
		manager = aManager;
	}

	public void run() {
		try {
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			manager.refreshScheduleServerInfo();
		} catch (Exception ex) {
			log.error(ex.getMessage(), ex);
		}
	}
}
package com.taobao.pamirs.schedule.taskmanager.task;

import java.util.Date;
import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.taskmanager.TBScheduleManager;
import com.taobao.pamirs.schedule.util.CronExpression;
import com.taobao.pamirs.schedule.util.ScheduleUtil;

/**
 *
 *
 * @author : lihx
 * create date : 2014-11-18
 */
public class PauseOrResumeScheduleTask extends java.util.TimerTask {
	private static transient Logger log = LoggerFactory.getLogger(HeartBeatTimerTask.class);
	public static int TYPE_PAUSE = 1;
	public static int TYPE_RESUME = 2;
	TBScheduleManager manager;
	Timer timer;
	int type;
	String cronTabExpress;
	public PauseOrResumeScheduleTask(TBScheduleManager aManager, Timer aTimer, int aType, String aCronTabExpress) {
		this.manager = aManager;
		this.timer = aTimer;
		this.type = aType;
		this.cronTabExpress = aCronTabExpress;
	}
	public void run() {
		try {
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			this.cancel();// 取消调度任务
			Date current = new Date(System.currentTimeMillis());
			CronExpression cexp = new CronExpression(this.cronTabExpress);
			Date nextTime = cexp.getNextValidTimeAfter(current);
			if (this.type == TYPE_PAUSE) {
				manager.pause("到达终止时间,pause调度");
				this.manager.getScheduleServer().setNextRunEndTime(ScheduleUtil.transferDataToString(nextTime));
			} else {
				manager.resume("到达开始时间,resume调度");
				this.manager.getScheduleServer().setNextRunStartTime(ScheduleUtil.transferDataToString(nextTime));
			}
			this.timer.schedule(new PauseOrResumeScheduleTask(this.manager, this.timer, this.type, this.cronTabExpress), nextTime);
		} catch (Throwable ex) {
			log.error(ex.getMessage(), ex);
		}
	}
}

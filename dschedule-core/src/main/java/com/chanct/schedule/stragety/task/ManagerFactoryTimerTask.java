package com.chanct.schedule.stragety.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chanct.schedule.stragety.TBSchedule;

/**
 * 
 * 
 * @author : lihx create date : 2014-11-18
 */
public/**
 * 用于定时检测与zookeeper连接，并刷新
 * @author lihx
 *
 */
class ManagerFactoryTimerTask extends java.util.TimerTask {
	private static transient Logger log = LoggerFactory.getLogger(ManagerFactoryTimerTask.class);
	TBSchedule manager;
	int count = 0;

	public ManagerFactoryTimerTask(TBSchedule manager) {
		this.manager = manager;
	}

	public void run() {
		try {
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			if (this.manager.getFactory().getZkManager().checkZookeeperState() == false) {
				if (count > 5) {
					log.error("Zookeeper连接失败，关闭所有的任务后，重新连接Zookeeper服务器......");
					this.manager.reStart();

				} else {
					count = count + 1;
				}
			} else {
				count = 0;
				this.manager.refresh();
			}
		} catch (Throwable ex) {
			log.error(ex.getMessage(), ex);
		}
	}
}

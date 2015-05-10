package com.taobao.pamirs.schedule.strategy.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.strategy.TBSchedule;

/**
 * 
 * 
 * @author : lihx create date : 2014-11-18
 */
public/**
 * ���ڶ�ʱ�����zookeeper���ӣ���ˢ��
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
					log.error("Zookeeper����ʧ�ܣ��ر����е��������������Zookeeper������......");
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

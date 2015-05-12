package com.chanct.schedule.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chanct.schedule.stragety.TBSchedule;
import com.chanct.schedule.zk.curator.CuratorManager;

public class ManagerWatcher implements Watcher {
	private static transient Logger log = LoggerFactory.getLogger(ManagerWatcher.class);
	private CuratorManager manager;
	private String watchPath;
	private TBSchedule schdeule = null;
	public ManagerWatcher(CuratorManager aManager, TBSchedule schedule) {
		this.manager = aManager;
		this.schdeule = schedule;
	}
	public void registerChildrenChanged(String path, Watcher watcher) throws Exception {
		manager.getZooKeeper().getChildren(path, true);
	}
	public void process(WatchedEvent event) {
		if (log.isInfoEnabled()) {
			log.info("已经触发了" + event.getType() + ":" + event.getState() + "事件！" + event.getPath());
		}
		if (event.getType() == Event.EventType.NodeChildrenChanged) {
			try {
				watchPath = event.getPath();
				this.manager.getZooKeeper().getChildren(watchPath, this);
				schdeule.setFactoryNumsChange(true);

			} catch (Exception e) {
				e.printStackTrace();
			}

		} else if (event.getState() == KeeperState.SyncConnected) {
			log.info("收到ZK连接成功事件！");
		} else if (event.getState() == KeeperState.Expired) {
			log.error("会话超时，等待重新建立ZK连接...");
			try {
				manager.reConnection();
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}
		}
	}
	public String getWatchPath() {
		return watchPath;
	}
	public void setWatchPath(String watchPath) {
		this.watchPath = watchPath;
	}
	
	
}
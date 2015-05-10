package com.taobao.pamirs.schedule.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.strategy.TBSchedule;
import com.taobao.pamirs.schedule.zk.curator.CuratorManager;

public class CuratorCacheWatcher implements Watcher {
	private static transient Logger log = LoggerFactory.getLogger(CuratorCacheWatcher.class);
	private CuratorManager manager;
	private String watchPath;
	private TBSchedule schdeule = null;
	public CuratorCacheWatcher(CuratorManager aManager, TBSchedule schedule) {
		this.manager = aManager;
		this.schdeule = schedule;
	}
	public void registerChildrenChanged(String path, Watcher watcher) throws Exception {
		manager.getZooKeeper().getChildren(path, true);
	}
	public void process(WatchedEvent event) {
		try {
			schdeule.setFactoryNumsChange(true);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	public String getWatchPath() {
		return watchPath;
	}
	public void setWatchPath(String watchPath) {
		this.watchPath = watchPath;
	}

}
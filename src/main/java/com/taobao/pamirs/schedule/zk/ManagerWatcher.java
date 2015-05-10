package com.taobao.pamirs.schedule.zk;

import java.io.FileReader;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.strategy.TBSchedule;
import com.taobao.pamirs.schedule.zk.curator.CuratorManager;

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
			log.info("�Ѿ�������" + event.getType() + ":" + event.getState() + "�¼���" + event.getPath());
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
			log.info("�յ�ZK���ӳɹ��¼���");
		} else if (event.getState() == KeeperState.Expired) {
			log.error("�Ự��ʱ���ȴ����½���ZK����...");
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
package com.chanct.schedule.zk.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;

/**
 * 
 * 
 * @author : lihx create date : 2014-12-9
 */
public class LostCStateListener implements ConnectionStateListener {
	private String zkRegPathPrefix;
	private String regContent;
	CuratorManager zkManager;
	public LostCStateListener(String zkRegPathPrefix, String regContent, CuratorManager manager) {
		this.zkRegPathPrefix = zkRegPathPrefix;
		this.regContent = regContent;
		this.zkManager = manager;
	}

	@Override
	public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
		if (connectionState == ConnectionState.LOST) {
			while (true) {
				try {
					if (curatorFramework.getZookeeperClient().blockUntilConnectedOrTimedOut()) {
						curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).withACL(zkManager.getAcl()).forPath(zkRegPathPrefix, regContent.getBytes("UTF-8"));
						break;
					}
				} catch (InterruptedException e) {
					break;
				} catch (Exception e) {
				}
			}
		}
	}
}

package com.taobao.pamirs.schedule.zk;

import java.io.FileReader;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.zk.curator.CuratorManager;

public class ScheduleWatcher implements Watcher {
	private static transient Logger log = LoggerFactory.getLogger(ScheduleWatcher.class);
	private Map<String,Watcher> route = new ConcurrentHashMap<String,Watcher>();
	private ZKManager manager;
	public ScheduleWatcher(ZKManager aManager){
		this.manager = aManager;
	}
	public void registerChildrenChanged(String path,Watcher watcher) throws Exception{
		manager.getZooKeeper().getChildren(path, true);
		route.put(path,watcher);
	}
	public void process(WatchedEvent event) {
		if(log.isInfoEnabled()){
			log.info("已经触发了" + event.getType() + ":"+ event.getState() + "事件！" + event.getPath());
		}
		if(event.getType() == Event.EventType.NodeChildrenChanged){
			String path = event.getPath();
			Watcher watcher = route.get(path);
			  if( watcher != null ){
				  try{
					  watcher.process(event);
				  }finally{
					  try{
						  if(manager.getZooKeeper().exists(path,null) != null){
							  manager.getZooKeeper().getChildren(path, true);
						  }
					  }catch(Exception e){
						  log.error(path +":" + e.getMessage(),e);
					  }
				  }
			  }else{
				  log.info("已经触发了" + event.getType() + ":"+ event.getState() + "事件！" + event.getPath());
			  }
		}else if (event.getState() == KeeperState.SyncConnected) {
			log.info("收到ZK连接成功事件！");
		} else if (event.getState() == KeeperState.Expired) {
			log.error("会话超时，等待重新建立ZK连接...");
			try {
				manager.reConnection();
			} catch (Exception e) {
				log.error(e.getMessage(),e);
			}
		}
	}
	public static void main(String[] args) throws Exception {
		String configFile =// System.getProperty("user.dir") + File.separator +
				 "F:\\workspaces\\schedule\\src\\main\\resources\\pamirsScheduleConfig.properties";
		Properties p = new Properties();
		FileReader reader = new FileReader(configFile);
		p.load(reader);
		reader.close();
		CuratorManager zkManager = new CuratorManager(p);
//		System.out.println(zkManager.getCurator().delete().deletingChildrenIfNeeded().inBackground().forPath("/taobao-pamirs-schedule/huijin/baseTaskType/group/group$group/runtime/20141202112750"));
		watchSubNodeChange(zkManager.getCurator(), "/taobao-pamirs-schedule/huijin/factory");
//		ZKTools.deleteTree(zkManager.getZooKeeper(), "/taobao-pamirs-schedule/huijin/baseTaskType/group/group$group/runtime");
//		ScheduleWatcher wat = new ScheduleWatcher(zkManager);
//
//		zkManager.getZooKeeper().getChildren("/taobao-pamirs-schedule/huijin/factory", wat);
//		Thread.sleep(Long.MAX_VALUE);
	}
	
	 public static void watchSubNodeChange(CuratorFramework client,String path) throws Exception {
	        PathChildrenCache cache = new PathChildrenCache(client, path, true);
	        cache.start(StartMode.POST_INITIALIZED_EVENT);
	        cache.getListenable().addListener(new PathChildrenCacheListener() {
	            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
	                switch (event.getType()) {
	                case CHILD_ADDED:
	                    System.out.println("Event : CHILD_ADDED");
	                    break;
	                case CHILD_UPDATED:
	                    System.out.println("Event : CHILD_UPDATED");
	                    break;
	                case CHILD_REMOVED:
	                    System.out.println("Event : CHILD_REMOVED");
	                    break;
	                default:
	                    break;
	                }
	            }
	        });
	        Thread.sleep(Integer.MAX_VALUE);
	    }
}
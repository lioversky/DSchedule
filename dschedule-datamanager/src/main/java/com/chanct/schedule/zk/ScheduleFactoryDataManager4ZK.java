package com.chanct.schedule.zk;

import java.io.Writer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.chanct.schedule.entity.strategy.ManagerFactoryInfo;
import com.chanct.schedule.entity.strategy.ScheduleStrategy;
import com.chanct.schedule.util.TimestampTypeAdapter;
import com.chanct.schedule.zk.curator.CuratorManager;
import com.chanct.schedule.zk.curator.LostCStateListener;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 
 * 
 * @author : lihx create date : 2014-11-19
 */
public class ScheduleFactoryDataManager4ZK {
	private String PATH_ManagerFactory;
	private Gson gson;
	private CuratorManager zkManager;
	public ScheduleFactoryDataManager4ZK(CuratorManager aZkManager) throws Exception {
		this.zkManager = aZkManager;
		gson = new GsonBuilder().registerTypeAdapter(Timestamp.class, new TimestampTypeAdapter()).setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		this.PATH_ManagerFactory = this.zkManager.getRootPath() + "/factory";

		if (this.getZooKeeper().exists(this.PATH_ManagerFactory, false) == null) {
			ZKTools.createPath(getZooKeeper(), this.PATH_ManagerFactory, CreateMode.PERSISTENT, this.zkManager.getAcl());
		}
	}

	/**
	 * 注册ManagerFactory
	 * 
	 * @param managerFactory
	 * @return 需要全部注销的调度，例如当IP不在列表中
	 * @throws Exception
	 */
	public void registerManagerFactory(ManagerFactoryInfo managerFactory) throws Exception {

		if (managerFactory.getUuid() == null) {
			String uuid = managerFactory.getIp() + "$" + managerFactory.getHostName() + "$" + UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();
			String zkPath = this.PATH_ManagerFactory + "/" + uuid + "$";
			zkPath = this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);
			managerFactory.setUuid(zkPath.substring(zkPath.lastIndexOf("/") + 1));
			ManagerFactoryInfo result = new ManagerFactoryInfo();
			result.setUuid(managerFactory.getUuid());
			result.setIp(managerFactory.getIp());
			result.setHostName(managerFactory.getHostName());
			result.setStart(true);
			LostCStateListener listener = new LostCStateListener(zkPath,this.gson.toJson(result),this.zkManager);
			this.zkManager.getCurator().getConnectionStateListenable().addListener(listener);
			this.getZooKeeper().setData(zkPath, this.gson.toJson(result).getBytes(), 0);
		} else {
			String zkPath = this.PATH_ManagerFactory + "/" + managerFactory.getUuid();
			if (this.getZooKeeper().exists(zkPath, false) == null) {
				ManagerFactoryInfo result = new ManagerFactoryInfo();
				result.setUuid(managerFactory.getUuid());
				result.setIp(managerFactory.getIp());
				result.setHostName(managerFactory.getHostName());
				result.setStart(true);
				zkPath = this.getZooKeeper().create(zkPath, this.gson.toJson(result).getBytes(), this.zkManager.getAcl(), CreateMode.EPHEMERAL);
//				this.getZooKeeper().setData(zkPath, this.gson.toJson(result).getBytes(), 0);
			}
		}
	}
	public void updateManagerFactoryInfo(String uuid, boolean isStart) throws Exception {
		String zkPath = this.PATH_ManagerFactory + "/" + uuid;
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			throw new Exception("任务管理器不存在:" + uuid);
		}
		ManagerFactoryInfo result = loadManagerFactoryInfo(uuid);
		result.setStart(isStart);
		this.getZooKeeper().setData(zkPath, this.gson.toJson(result).getBytes(), -1);
	}
	
	public void updateManagerFactoryInfo(ManagerFactoryInfo info) throws Exception {
		String zkPath = this.PATH_ManagerFactory + "/" + info.getUuid();
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			throw new Exception("任务管理器不存在:" + info.getUuid());
		}
		this.getZooKeeper().setData(zkPath, this.gson.toJson(info).getBytes(), -1);
	}

	public ManagerFactoryInfo loadManagerFactoryInfo(String uuid) throws Exception {
		String zkPath = this.PATH_ManagerFactory + "/" + uuid;
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			throw new Exception("任务管理器不存在:" + uuid);
		}
		byte[] value = this.getZooKeeper().getData(zkPath, false, null);
		ManagerFactoryInfo result = new ManagerFactoryInfo();
		if (value == null) {
			result.setUuid(uuid);
			result.setStart(true);
		} else {
			result = this.gson.fromJson(new String(value), ManagerFactoryInfo.class);
		}
		return result;
	}
	
	
	/**
	 * 加载机器列表
	 * @return
	 * @throws Exception
	 */
	public List<String> loadAllFactory() throws Exception {
		String zkPath = this.PATH_ManagerFactory;
		List<String> names = this.getZooKeeper().getChildren(zkPath, false);
		Collections.sort(names, new Comparator<String>() {
			public int compare(String u1, String u2) {
				return u1.substring(u1.lastIndexOf("$") + 1).compareTo(u2.substring(u2.lastIndexOf("$") + 1));
			}
		});

		return names;
	}
	
	
	/**
	 * 检测策略是否可以在机器上运行
	 * @param scheduleStrategy 策略
	 * @param uuid 机器id
	 * @return 
	 */
	public boolean checkFactoryCanRun(ScheduleStrategy scheduleStrategy, String uuid) throws Exception {
		ManagerFactoryInfo factory = loadManagerFactoryInfo(uuid);
		if (scheduleStrategy.getIPList() == null)
			return true;
		for (String ip : scheduleStrategy.getIPList()) {
			if (ip.equals("127.0.0.1") || ip.equalsIgnoreCase("localhost") || ip.equals(factory.getIp()) || ip.equalsIgnoreCase(factory.getHostName())) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 加载所有机器列表及信息
	 * @return
	 * @throws Exception
	 */
	public List<ManagerFactoryInfo> loadAllManagerFactoryInfo() throws Exception {
		List<ManagerFactoryInfo> result = new ArrayList<ManagerFactoryInfo>();
		List<String> names = loadAllFactory();
		for (String name : names) {
			ManagerFactoryInfo info = loadManagerFactoryInfo(name);
			result.add(info);
		}
		return result;
	}
	
	public boolean isLeader(String uuid,List<String> factoryList){
    	return uuid.equals(getLeader(factoryList));
    }
	/**
	 * 最小的是server
	 */
	public String getLeader(List<String> factoryList) {
		if (factoryList == null || factoryList.size() == 0) {
			return "";
		}
		long no = Long.MAX_VALUE;
		long tmpNo = -1;
		String leader = null;
		for (String factory : factoryList) {
			tmpNo = Long.parseLong(factory.substring(factory.lastIndexOf("$") + 1));
			if (no > tmpNo) {
				no = tmpNo;
				leader = factory;
			}
		}
		return leader;
	}

	public void watchFactory(final Watcher watcher) throws Exception {
		// this.getZooKeeper().getChildren(this.PATH_ManagerFactory, watcher);
		PathChildrenCache cache = new PathChildrenCache(this.zkManager.getCurator(), this.PATH_ManagerFactory, true);
		cache.start(StartMode.POST_INITIALIZED_EVENT);
		cache.getListenable().addListener(new PathChildrenCacheListener() {
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				switch (event.getType()) {
					case CHILD_ADDED :
						watcher.process(null);
						break;
					// case CHILD_UPDATED:
					// System.out.println("Event : CHILD_UPDATED");
					// break;
					case CHILD_REMOVED :
						watcher.process(null);
						break;
					default :
						break;
				}
			}
		});
	}
	public void printTree(String path, Writer writer, String lineSplitChar) throws Exception {
		ZKTools.printTree(this.getZooKeeper(), path, writer, lineSplitChar);
	}
	public void deleteTree(String path) throws Exception {
		ZKTools.deleteTree(this.getZooKeeper(), path);
	}
	public ZooKeeper getZooKeeper() throws Exception {
		return this.zkManager.getZooKeeper();
	}
	public String getRootPath() {
		return this.zkManager.getRootPath();
	}
}

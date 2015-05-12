package com.chanct.schedule.datamanager;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chanct.schedule.entity.TaskItemDefine;
import com.chanct.schedule.entity.manager.ScheduleServer;
import com.chanct.schedule.entity.manager.ScheduleTaskItem;
import com.chanct.schedule.entity.manager.ScheduleTaskItem.TaskItemSts;
import com.chanct.schedule.entity.manager.ScheduleTaskType;
import com.chanct.schedule.entity.manager.ScheduleTaskTypeRunningInfo;
import com.chanct.schedule.util.ScheduleUtil;
import com.chanct.schedule.util.TimestampTypeAdapter;
import com.chanct.schedule.zk.curator.CuratorManager;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 
 * 
 * @author : lihx create date : 2014-12-2
 */
public class ScheduleDataManager4Curator implements IScheduleDataManager {
	private static transient Logger log = LoggerFactory.getLogger(ScheduleDataManager4Curator.class);
	private Gson gson;
	private CuratorManager zkManager;
	private String PATH_BaseTaskType;
	private String PATH_TaskItem = "taskItem";
	private String PATH_Server = "server";
	private long zkBaseTime = 0;
	private long loclaBaseTime = 0;
	public ScheduleDataManager4Curator(CuratorManager aZkManager) throws Exception {
		this.zkManager = aZkManager;
		gson = new GsonBuilder().registerTypeAdapter(Timestamp.class, new TimestampTypeAdapter()).setDateFormat("yyyy-MM-dd HH:mm:ss").create();

		this.PATH_BaseTaskType = this.zkManager.getRootPath() + "/baseTaskType";
		// 创建baseTaskType节点

		if (zkManager.getCurator().checkExists().forPath(PATH_BaseTaskType) == null) {
			zkManager.getCurator().create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(this.zkManager.getAcl()).forPath(PATH_BaseTaskType);
		}
		loclaBaseTime = System.currentTimeMillis();
		String tempPath = this.zkManager.getZooKeeper().create(this.zkManager.getRootPath() + "/systime", null, this.zkManager.getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);
		Stat tempStat = this.zkManager.getZooKeeper().exists(tempPath, false);
		zkBaseTime = tempStat.getCtime();
		zkManager.getCurator().delete().deletingChildrenIfNeeded().forPath(tempPath);
		if (Math.abs(this.zkBaseTime - this.loclaBaseTime) > 5000) {
			log.error("请注意，Zookeeper服务器时间与本地时间相差 ： " + Math.abs(this.zkBaseTime - this.loclaBaseTime) + " ms");
		}
	}
	@Override
	public long getSystemTime() {
		return this.zkBaseTime + (System.currentTimeMillis() - this.loclaBaseTime);
	}

	@Override
	public List<TaskItemDefine> reloadDealTaskItem(String taskType, String uuid) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;

		List<String> taskItems = zkManager.getCurator().getChildren().forPath(zkPath);
		Collections.sort(taskItems);

		List<TaskItemDefine> result = new ArrayList<TaskItemDefine>();
		for (String name : taskItems) {

			byte[] value = zkManager.getCurator().getData().forPath(zkPath + "/" + name + "/cur_server");
			if (value != null && uuid.equals(new String(value))) {
				TaskItemDefine item = new TaskItemDefine();
				item.setTaskItemId(name);
				byte[] parameterValue = zkManager.getCurator().getData().forPath(zkPath + "/" + name + "/parameter");
				if (parameterValue != null) {
					item.setParameter(new String(parameterValue));
				}
				result.add(item);
			}
		}
		return result;
	}

	@Override
	public List<ScheduleTaskItem> loadAllTaskItem(String taskType) throws Exception {
		List<ScheduleTaskItem> result = new ArrayList<ScheduleTaskItem>();
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;

		if (zkManager.getCurator().checkExists().forPath(zkPath) == null) {
			return result;
		}

		List<String> taskItems = zkManager.getCurator().getChildren().forPath(zkPath);
		Collections.sort(taskItems);
		for (String taskItem : taskItems) {
			ScheduleTaskItem info = new ScheduleTaskItem();
			info.setTaskType(taskType);
			info.setTaskItem(taskItem);
			String zkTaskItemPath = zkPath + "/" + taskItem;

			byte[] curContent = zkManager.getCurator().getData().forPath(zkTaskItemPath + "/cur_server");
			if (curContent != null) {
				info.setCurrentScheduleServer(new String(curContent));
			}
			byte[] reqContent = zkManager.getCurator().getData().forPath(zkTaskItemPath + "/req_server");
			if (reqContent != null) {
				info.setRequestScheduleServer(new String(reqContent));
			}
			byte[] stsContent = zkManager.getCurator().getData().forPath(zkTaskItemPath + "/sts");
			if (stsContent != null) {
				info.setSts(ScheduleTaskItem.TaskItemSts.valueOf(new String(stsContent)));
			}
			byte[] parameterContent = zkManager.getCurator().getData().forPath(zkTaskItemPath + "/parameter");
			if (parameterContent != null) {
				info.setDealParameter(new String(parameterContent));
			}
			byte[] dealDescContent = zkManager.getCurator().getData().forPath(zkTaskItemPath + "/deal_desc");
			if (dealDescContent != null) {
				info.setDealDesc(new String(dealDescContent));
			}
			result.add(info);
		}
		return result;

	}

	@Override
	public void releaseDealTaskItem(String taskType, String uuid) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
		boolean isModify = false;

		for (String name : zkManager.getCurator().getChildren().forPath(zkPath)) {
			byte[] curServerValue = zkManager.getCurator().getData().forPath(zkPath + "/" + name + "/cur_server");
			byte[] reqServerValue = zkManager.getCurator().getData().forPath(zkPath + "/" + name + "/req_server");
			if (reqServerValue != null && curServerValue != null && uuid.equals(new String(curServerValue)) == true) {
				zkManager.getCurator().setData().withVersion(-1).forPath(zkPath + "/" + name + "/cur_server", reqServerValue);
				zkManager.getCurator().setData().withVersion(-1).forPath(zkPath + "/" + name + "/req_server", null);
				isModify = true;
			}
		}
		if (isModify == true) { // 设置需要所有的服务器重新装载任务
			this.updateReloadTaskItemFlag(taskType);
		}
	}

	@Override
	public int queryTaskItemCount(String taskType) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;
		return zkManager.getCurator().getChildren().forPath(zkPath).size();
	}

	@Override
	public ScheduleTaskType loadTaskTypeBaseInfo(String baseTaskType) throws Exception {
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType;
		
		if (zkManager.getCurator().checkExists().forPath(zkPath) == null) {
			return null;
		}
		byte[] data = zkManager.getCurator().getData().forPath(zkPath);
		if (data != null) {
			String valueString = new String(data);
			ScheduleTaskType result = (ScheduleTaskType) this.gson.fromJson(valueString, ScheduleTaskType.class);
			return result;
		} else
			return null;
	}

	@Override
	public int clearExpireScheduleServer(String taskType, long expireTime) throws Exception {
		int result = 0;
		// 不存在，创建节点
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
		if (zkManager.getCurator().checkExists().forPath(zkPath) == null) {
//			String tempPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType;
//			if (zkManager.getCurator().checkExists().forPath(tempPath) == null) {
//				zkManager.getCurator().create().withMode(CreateMode.PERSISTENT).withACL(this.zkManager.getAcl()).forPath(tempPath);
//			}
			zkManager.getCurator().create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(this.zkManager.getAcl()).forPath(zkPath);
		}
		// 超时清理
		for (String name : zkManager.getCurator().getChildren().forPath(zkPath)) {
			try {
				Stat stat = zkManager.getCurator().checkExists().forPath(zkPath + "/" + name);
				if (getSystemTime() - stat.getMtime() > expireTime) {
					zkManager.getCurator().delete().deletingChildrenIfNeeded().forPath(zkPath + "/" + name);
					result++;
				}
			} catch (Exception e) {
				// 当有多台服务器时，存在并发清理的可能，忽略异常
				result++;
			}
		}
		return result;
	}

	@Override
	public int clearTaskItem(String taskType, List<String> serverList) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_TaskItem;

		int result = 0;
		for (String name : zkManager.getCurator().getChildren().forPath(zkPath)) {
			byte[] curServerValue = zkManager.getCurator().getData().forPath(zkPath + "/" + name + "/cur_server");
			if (curServerValue != null) {
				String curServer = new String(curServerValue);
				boolean isFind = false;
				for (String server : serverList) {
					if (curServer.equals(server)) {
						isFind = true;
						break;
					}
				}
				if (isFind == false) {
					zkManager.getCurator().setData().withVersion(-1).forPath(zkPath + "/" + name + "/cur_server");
					result = result + 1;
				}
			} else {
				result = result + 1;
			}
		}
		return result;
	}

	@Override
	public List<ScheduleServer> selectAllValidScheduleServer(String taskType) throws Exception {
		List<ScheduleServer> result = new ArrayList<ScheduleServer>();
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + this.PATH_Server;
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			return result;
		}
		List<String> serverList = this.getZooKeeper().getChildren(zkPath, false);
		Collections.sort(serverList, new Comparator<String>() {
			public int compare(String u1, String u2) {
				return u1.substring(u1.lastIndexOf("$") + 1).compareTo(u2.substring(u2.lastIndexOf("$") + 1));
			}
		});
		for (String name : serverList) {
			try {
				String valueString = new String(this.getZooKeeper().getData(zkPath + "/" + name, false, null));
				ScheduleServer server = (ScheduleServer) this.gson.fromJson(valueString, ScheduleServer.class);
				server.setCenterServerTime(new Timestamp(this.getSystemTime()));
				result.add(server);
			} catch (Exception e) {
				log.debug(e.getMessage(), e);
			}
		}
		return result;
	}

	@Override
	public List<String> loadScheduleServerNames(String taskType) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void assignTaskItem(String taskType, String currentUuid, int maxNumOfOneServer, List<String> serverList) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean refreshScheduleServer(ScheduleServer server) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void registerScheduleServer(ScheduleServer server) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void unRegisterScheduleServer(String taskType, String serverUUID) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearExpireTaskTypeRunningInfo(String baseTaskType, String serverUUID, double expireDateInternal) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isLeader(String uuid, List<String> serverList) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void pauseAllServer(String baseTaskType) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void resumeAllServer(String baseTaskType) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public List<ScheduleTaskType> getAllTaskTypeBaseInfo() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearTaskType(String baseTaskType) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void createBaseTaskType(ScheduleTaskType baseTaskType) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBaseTaskType(ScheduleTaskType baseTaskType) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void createGroupTaskType(ScheduleTaskType baseTaskType) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateGroupTaskType(ScheduleTaskType baseTaskType) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public List<ScheduleTaskTypeRunningInfo> getAllTaskTypeRunningInfo(String baseTaskType) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteTaskType(String baseTaskType) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public List<ScheduleServer> selectScheduleServer(String baseTaskType, String ownSign, String ip, String orderStr) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ScheduleServer> selectHistoryScheduleServer(String baseTaskType, String ownSign, String ip, String orderStr) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ScheduleServer> selectScheduleServerByManagerFactoryUUID(String factoryUUID) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void createScheduleTaskItem(ScheduleTaskItem[] taskItems) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateScheduleTaskItemStatus(String taskType, String taskItem, TaskItemSts sts, String message) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteScheduleTaskItem(String taskType, String taskItem) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void initialRunningInfo4Static(String baseTaskType, String ownSign, String uuid) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void initialRunningInfo4Dynamic(String baseTaskType, String ownSign) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isInitialRunningInfoSucuss(String baseTaskType, String ownSign) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setInitialRunningInfoSucuss(String baseTaskType, String taskType, String uuid) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public String getLeader(List<String> serverList) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long updateReloadTaskItemFlag(String taskType) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getReloadTaskItemFlag(String taskType) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void watchItem(String taskType, String item, Watcher watcher) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public ScheduleTaskType loadGroupTaskTypeBaseInfo(String baseTaskType, String uuid) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Integer> loadGroupMapInfo(String baseTaskType) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	public ZooKeeper getZooKeeper() throws Exception {
		return this.zkManager.getZooKeeper();
	}
}

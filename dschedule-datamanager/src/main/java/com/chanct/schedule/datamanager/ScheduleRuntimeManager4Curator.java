package com.chanct.schedule.datamanager;

import java.sql.Timestamp;
import java.util.List;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chanct.schedule.entity.manager.ScheduleTaskItem;
import com.chanct.schedule.entity.manager.ScheduleTaskTypeRunningInfo;
import com.chanct.schedule.util.ScheduleUtil;
import com.chanct.schedule.util.TimestampTypeAdapter;
import com.chanct.schedule.zk.curator.CuratorManager;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 
 * 
 * @author : lihx create date : 2014-12-3
 */
public class ScheduleRuntimeManager4Curator implements IScheduleRuntimeManager {

	public static final String SP = "/";
	private static transient Logger log = LoggerFactory.getLogger(ScheduleRuntimeManager4Curator.class);
	private Gson gson;

	private CuratorManager zkManager;
	private String PATH_BaseTaskType;
	private String PATH_Runtime = "runtime";
	public ScheduleRuntimeManager4Curator(CuratorManager aZkManager) {
		this.zkManager = aZkManager;
		gson = new GsonBuilder().registerTypeAdapter(Timestamp.class, new TimestampTypeAdapter()).setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		this.PATH_BaseTaskType = this.zkManager.getRootPath() + "/baseTaskType";

	}

	@Override
	public void createRunTimeSts(String taskType, String taskName, String time) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		StringBuilder sb = new StringBuilder();
		sb.append(this.PATH_BaseTaskType).append(SP).append(baseTaskType).append(SP).append(taskType).append(SP).append(PATH_Runtime).append(SP).append(time).append(SP).append(taskName);

		if (zkManager.getCurator().checkExists().forPath(sb.toString()) == null) {
			zkManager.getCurator().create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(this.zkManager.getAcl())
					.forPath(sb.toString(), ScheduleTaskItem.TaskItemSts.ACTIVTE.toString().getBytes());
		} else {
			throw new Exception("执行任务" + time + SP + taskName + "已经存在,如果确认重新执行，请先调用deleteTaskType(String baseTaskType)删除");
		}
	}

	@Override
	public void finishRunTimeItem(String taskType, String taskName, String time) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		StringBuilder sb = new StringBuilder();
		sb.append(this.PATH_BaseTaskType).append(SP).append(baseTaskType).append(SP).append(taskType).append(SP).append(PATH_Runtime).append(SP).append(time).append(SP).append(taskName);
		// String zkPath = this.PATH_BaseTaskType + SP + baseTaskType + SP + taskType + SP +
		// PATH_Runtime + SP + time+ SP + taskName;
		if (zkManager.getCurator().checkExists().forPath(sb.toString()) == null) {
			throw new Exception("执行任务" + sb.toString() + "已经完成，但zookeeper中不存在此节点！");
		} else {
			zkManager.getCurator().setData().withVersion(0).forPath(sb.toString(), ScheduleTaskItem.TaskItemSts.FINISH.toString().getBytes());
		}
	}

	@Override
	public void deleteFinishNode(String taskType, String time) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		StringBuilder sb = new StringBuilder();
		sb.append(this.PATH_BaseTaskType).append(SP).append(baseTaskType).append(SP).append(taskType).append(SP).append(PATH_Runtime).append(SP).append(time);
		if (zkManager.getCurator().checkExists().forPath(sb.toString()) != null) {
			zkManager.getCurator().delete().deletingChildrenIfNeeded().forPath(sb.toString());
		}
	}

	@Override
	public ScheduleTaskTypeRunningInfo loadRunningInfo(String taskType, String taskName, String time) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		StringBuilder sb = new StringBuilder();
		sb.append(this.PATH_BaseTaskType).append(SP).append(baseTaskType).append(SP).append(taskType).append(SP).append(PATH_Runtime).append(SP).append(time).append(SP).append(taskName);

		return loadRunningInfo(sb.toString());
	}

	@Override
	public ScheduleTaskTypeRunningInfo loadRunningInfo(String zkPath) throws Exception {
		ScheduleTaskTypeRunningInfo info = new ScheduleTaskTypeRunningInfo();

		byte[] value = zkManager.getCurator().getData().forPath(zkPath);
		if (value != null) {
			info.setSts(ScheduleTaskItem.TaskItemSts.valueOf(new String(value)));
		}
		return info;
	}

	@Override
	public List<ScheduleTaskTypeRunningInfo> loadAllRunningInfo(String taskType, String taskName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean watchPreviousItem(String taskType, String previousTask, String time, CuratorWatcher watcher, int index) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		StringBuilder sb = new StringBuilder();
		sb.append(this.PATH_BaseTaskType).append(SP).append(baseTaskType).append(SP).append(taskType).append(SP).append(PATH_Runtime).append(SP).append(time).append(SP).append(previousTask);

		if (zkManager.getCurator().checkExists().forPath(sb.toString()) == null) {
			zkManager.getCurator().checkExists().usingWatcher(watcher).forPath(sb.toString());
		} else {
			byte[] value = zkManager.getCurator().getData().forPath(sb.toString());
			if (value != null && ScheduleTaskItem.TaskItemSts.FINISH.equals(ScheduleTaskItem.TaskItemSts.valueOf(new String(value)))) {
				return false;
			}
			zkManager.getCurator().getData().usingWatcher(watcher).forPath(sb.toString());
		}
		return true;
	}
	
	@Override
	public boolean watchPreviousItem(String taskType, String previousTask, String time, Watcher watcher, int index) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	public ZooKeeper getZooKeeper() throws Exception {
		return this.zkManager.getZooKeeper();
	}
}

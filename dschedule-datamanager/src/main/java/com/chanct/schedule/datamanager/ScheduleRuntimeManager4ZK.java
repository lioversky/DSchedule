package com.chanct.schedule.datamanager;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.chanct.schedule.zk.ZKTools;
import com.chanct.schedule.zk.curator.CuratorManager;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 *记录任务的执行日志、状态接口实现类
 *
 * @author : lihx
 * create date : 2014-12-1
 */
public class ScheduleRuntimeManager4ZK implements IScheduleRuntimeManager {
	
	private static transient Logger log = LoggerFactory.getLogger(ScheduleRuntimeManager4ZK.class);
	private Gson gson ;
	
	private CuratorManager zkManager;
	private String PATH_BaseTaskType;
	private String PATH_Runtime = "runtime";
	private String PATH_TaskItem = "taskItem";
	public ScheduleRuntimeManager4ZK(CuratorManager aZkManager) {
		this.zkManager = aZkManager;
		gson = new GsonBuilder().registerTypeAdapter(Timestamp.class,new TimestampTypeAdapter()).setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		this.PATH_BaseTaskType = this.zkManager.getRootPath() + "/baseTaskType";
		
	}
	
	@Override
	public void createRunTimeSts(String taskType, String taskName, String time) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + PATH_Runtime;
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
		}
		String datePath = zkPath + "/" + time;
		if (this.getZooKeeper().exists(datePath, false) == null) {
			this.getZooKeeper().create(datePath, null, this.zkManager.getAcl(), CreateMode.PERSISTENT);
		}

		String taskPath = datePath + "/" + taskName;
		if (this.getZooKeeper().exists(taskPath, false) == null) {
			this.getZooKeeper().create(taskPath, ScheduleTaskItem.TaskItemSts.ACTIVTE.toString().getBytes(), this.zkManager.getAcl(), CreateMode.PERSISTENT);
		} else {
			throw new Exception("执行任务" + time + "/" + taskName + "已经存在,如果确认重新执行，请先调用deleteTaskType(String baseTaskType)删除");
		}
	}

	@Override
	public void finishRunTimeItem(String taskType, String taskName, String time) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + PATH_Runtime + "/" + time+ "/" + taskName;
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			throw new Exception("执行任务" + zkPath + "已经完成，但zookeeper中不存在此节点！");
		}else {
			this.getZooKeeper().setData(zkPath, ScheduleTaskItem.TaskItemSts.FINISH.toString().getBytes(), 0);
		}
	}

	@Override
	public void deleteFinishNode(String taskType, String time) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + PATH_Runtime + "/" + time;
		if (this.getZooKeeper().exists(zkPath, false) != null) {
			ZKTools.deleteTree(this.getZooKeeper(), zkPath);
		}
	}

	@Override
	public ScheduleTaskTypeRunningInfo loadRunningInfo(String taskType, String taskName, String time) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + PATH_Runtime + "/" + time + "/" + taskName;

		return loadRunningInfo(zkPath);
	}

	@Override
	public ScheduleTaskTypeRunningInfo loadRunningInfo(String zkPath) throws Exception {
		ScheduleTaskTypeRunningInfo info = new ScheduleTaskTypeRunningInfo();

		byte[] value = this.getZooKeeper().getData(zkPath, false, null);
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
	public boolean watchPreviousItem(String taskType, String previousTask, String time, Watcher watcher, int index) throws Exception {
		String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(taskType);
		String zkPath = this.PATH_BaseTaskType + "/" + baseTaskType + "/" + taskType + "/" + PATH_Runtime + "/" + time;
		zkPath = zkPath + "/" + previousTask;
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			this.getZooKeeper().exists(zkPath, watcher);
		} else {
			byte[] value = this.getZooKeeper().getData(zkPath, null, null);
			if (value != null && ScheduleTaskItem.TaskItemSts.FINISH.equals(ScheduleTaskItem.TaskItemSts.valueOf(new String(value)))) {
				return false;
			}
			this.getZooKeeper().getData(zkPath, watcher, null);
		}

		return true;
	}
	
	@Override
	public boolean watchPreviousItem(String taskType, String previousTask, String time, CuratorWatcher watcher, int index) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	public ZooKeeper getZooKeeper() throws Exception {
		return this.zkManager.getZooKeeper();
	}
	public static void main(String[] args) {
		Gson g = new Gson();
		Map<String,Integer> map = new HashMap<String,Integer>();
		map.put("group-test-1", 1);
		map.put("group-test-2", 2);
		map.put("group-test-3", 3);
		System.out.println(g.toJson(map));
	}
}

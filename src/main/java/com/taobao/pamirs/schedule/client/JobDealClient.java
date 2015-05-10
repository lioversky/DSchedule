package com.taobao.pamirs.schedule.client;

import java.io.FileReader;
import java.io.InputStream;
import java.util.Properties;

import org.apache.zookeeper.CreateMode;

import com.google.gson.Gson;
import com.taobao.pamirs.schedule.entity.manager.ScheduleTaskType;
import com.taobao.pamirs.schedule.entity.strategy.ScheduleStrategy;
import com.taobao.pamirs.schedule.zk.curator.CuratorManager;

/**
 * 
 * 
 * @author : lihx create date : 2014-12-3
 */
public class JobDealClient {
	private static CuratorManager zkManager;
	private static void setZkManager(CuratorManager zkManager) {
		JobDealClient.zkManager = zkManager;
	}
	private static final String dealBean = "parameterTaskBean";
	private static final long heartBeatRate = 10000;
	private static final long judgeDeadInterval = 60000;
	private static final int threadNumber = 1;
	private static final String processType = "NORMAL";
	private static final int executeNumber = 1;
	private static final String permitRunStartTime = "0 0/1 * * * ?";

	private static final int maxTaskItemsOfOneThreadGroup = 1;

	private static CuratorManager getClientInstence() throws Exception {
		if (zkManager == null) {
			InputStream is = null;

			Properties p = new Properties();
			try {
				is = JobDealClient.class.getClassLoader().getResourceAsStream("pamirsScheduleConfig.properties");
				p.load(is);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (is != null) {
					is.close();
				}
			}
			zkManager = new CuratorManager(p);
			zkManager.initial();
		}
		return zkManager;
	}

	private static boolean createTaskJob(String json, String jobName, long taskId) throws Exception {
		ScheduleTaskType taskType = new ScheduleTaskType();
		String baseTaskType = jobName + "-" + taskId;
		taskType.setBaseTaskType(baseTaskType);
		taskType.setDealBeanName(dealBean);
		taskType.setHeartBeatRate(heartBeatRate);
		taskType.setJudgeDeadInterval(judgeDeadInterval);
		taskType.setThreadNumber(threadNumber);
		taskType.setExecuteNumber(executeNumber);
		taskType.setProcessorType(processType);
		// taskType.setExpireOwnSignInterval("expireOwnSignInterval")==null?0:
		// Integer.parseInt("threadNumber")));
		taskType.setTaskParameter(json);
		taskType.setPermitRunStartTime(permitRunStartTime);
		taskType.setMaxTaskItemsOfOneThreadGroup(maxTaskItemsOfOneThreadGroup);
		taskType.setTaskItems(new String[]{"0"});
		createBaseTaskType(taskType);
		return true;
	}

	private static boolean createStrategyJob(String jobName, long taskId) throws Exception {
		ScheduleStrategy scheduleStrategy = new ScheduleStrategy();
		String strategyName = jobName + "-" + taskId;
		scheduleStrategy.setStrategyName(strategyName);
		scheduleStrategy.setKind(ScheduleStrategy.Kind.Schedule);
		scheduleStrategy.setTaskName(strategyName + "$safecity");
		scheduleStrategy.setTaskParameter(null);

		scheduleStrategy.setNumOfSingleServer(1);
		scheduleStrategy.setAssignNum(1);
		createScheduleStrategy(scheduleStrategy);
		return true;
	}
	private static void createScheduleStrategy(ScheduleStrategy scheduleStrategy) throws Exception {
		String zkPath = getClientInstence().getRootPath() + "/strategy" + "/" + scheduleStrategy.getStrategyName();
		String valueString = new Gson().toJson(scheduleStrategy);
		if (getClientInstence().getCurator().checkExists().forPath(zkPath) == null) {
			getClientInstence().getCurator().create().withMode(CreateMode.PERSISTENT).withACL(getClientInstence().getAcl()).forPath(zkPath, valueString.getBytes());
		} else {
			throw new Exception("调度策略" + scheduleStrategy.getStrategyName() + "已经存在,如果确认需要重建，请先调用deleteMachineStrategy(String taskType)删除");
		}
	}
	private static void createBaseTaskType(ScheduleTaskType baseTaskType) throws Exception {
		if (baseTaskType.getBaseTaskType().indexOf("$") > 0) {
			throw new Exception("调度任务" + baseTaskType.getBaseTaskType() + "名称不能包括特殊字符 $");
		}
		String zkPath = getClientInstence().getRootPath() + "/baseTaskType" + "/" + baseTaskType.getBaseTaskType();
		String valueString = new Gson().toJson(baseTaskType);
		if (getClientInstence().getCurator().checkExists().forPath(zkPath) == null) {
			getClientInstence().getCurator().create().withMode(CreateMode.PERSISTENT).withACL(getClientInstence().getAcl()).forPath(zkPath, valueString.getBytes());
		} else {
			throw new Exception("调度任务" + baseTaskType.getBaseTaskType() + "已经存在,如果确认需要重建，请先调用deleteTaskType(String baseTaskType)删除");
		}
	}

	public static boolean createScheduleJob(String json, String jobName, long taskId) throws Exception {
		if (createTaskJob(json, jobName, taskId)) {
			if (createStrategyJob(jobName, taskId)) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean createScheduleJob(ScheduleTaskType task,ScheduleStrategy Strategy) throws Exception{
		createBaseTaskType(task);
		createScheduleStrategy(Strategy);
		return true;
	}
	
	public static boolean stopScheduleJob(String jobName, String taskId) throws Exception {
		pause(jobName + "-" + taskId);
		return false;
	}
	
	private static boolean pause(String strategyName) throws Exception {
		ScheduleStrategy strategy = loadStrategy(strategyName);
		strategy.setSts(ScheduleStrategy.STS_PAUSE);
		updateScheduleStrategy(strategy);
		return true;
	}
	
	public static ScheduleStrategy loadStrategy(String strategyName) throws Exception {
		String zkPath = getClientInstence().getRootPath() + "/strategy" + "/" + strategyName;
		if (getClientInstence().getCurator().checkExists().forPath(zkPath) == null) {
			return null;
		}
		String valueString = new String(getClientInstence().getCurator().getData().forPath(zkPath));
		ScheduleStrategy result = (ScheduleStrategy) new Gson().fromJson(valueString, ScheduleStrategy.class);
		return result;
	}
	public static void updateScheduleStrategy(ScheduleStrategy scheduleStrategy) throws Exception {
		String zkPath = getClientInstence().getRootPath() + "/strategy" + "/" + scheduleStrategy.getStrategyName();
		String valueString = new Gson().toJson(scheduleStrategy);
		if (getClientInstence().getCurator().checkExists().forPath(zkPath) == null) {
			getClientInstence().getCurator().create().withMode(CreateMode.PERSISTENT).withACL(getClientInstence().getAcl()).forPath(zkPath, valueString.getBytes());
		} else {
			getClientInstence().getCurator().setData().withVersion(-1).forPath(zkPath, valueString.getBytes());
		}

	}
	public static void main(String[] args) {
		try {
//			createScheduleJob("jobName-5", "jobName", 5);
			stopScheduleJob("kafka", "task");
			getClientInstence().close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

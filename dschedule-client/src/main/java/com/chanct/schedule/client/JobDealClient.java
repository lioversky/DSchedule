package com.chanct.schedule.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.chanct.schedule.datamanager.IScheduleDataManager;
import com.chanct.schedule.datamanager.ScheduleDataManager4ZK;
import com.chanct.schedule.entity.manager.ScheduleTaskType;
import com.chanct.schedule.entity.strategy.ScheduleStrategy;
import com.chanct.schedule.zk.ScheduleStrategyDataManager4ZK;
import com.chanct.schedule.zk.curator.CuratorManager;

/**
 * 
 * 
 * @author : lihx create date : 2014-12-3
 */
public class JobDealClient {
	private static CuratorManager zkManager;
	private static IScheduleDataManager scheduleDataManager;
	private static ScheduleStrategyDataManager4ZK scheduleStrategyManager;
	
	 static  {
		try {
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
				scheduleDataManager = new ScheduleDataManager4ZK(zkManager);
				scheduleStrategyManager = new ScheduleStrategyDataManager4ZK(zkManager);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	
	/**
	 * 创建任务
	 * @param task任务项
	 * @param Strategy 策略
	 * @return
	 * @throws Exception
	 */
	public boolean createScheduleJob(ScheduleTaskType task, ScheduleStrategy Strategy) {
		try {
			scheduleDataManager.createGroupTaskType(task);
			scheduleStrategyManager.createScheduleStrategy(Strategy);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public List<ScheduleStrategy> selectScheduleStrategyList(String ownSign) {
		List<ScheduleStrategy> list = null;
		try {
			list = scheduleStrategyManager.loadAllScheduleStrategy();
			Iterator<ScheduleStrategy> iterator = list.iterator();
			for (ScheduleStrategy strategy = iterator.next(); iterator.hasNext();) {
				if (!ownSign.equals(strategy.getOwnSign())) {
					iterator.remove();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	
	
	
	public ScheduleStrategy selectScheduleStrategy(String strategyName) throws Exception {
		return scheduleStrategyManager.loadStrategy(strategyName);
	}
	
	public  ScheduleTaskType selectScheduleTask(String taskName) throws Exception {
		return scheduleDataManager.loadTaskTypeBaseInfo(taskName);
	}
	
	public  boolean pause(String strategyName) throws Exception {
		scheduleStrategyManager.pause(strategyName);
		return true;
	}
	public boolean resume(String strategyName) throws Exception {
		scheduleStrategyManager.resume(strategyName);
		return true;
	}
	
	
	
	public static void main(String[] args) {
		try {
			
			/*ScheduleTaskType taskType = new ScheduleTaskType();
			taskType.setBaseTaskType("taskType");
			taskType.setDealBeanName("org.");
			taskType.setKind(ScheduleTaskType.Kind.Normal);
			//taskType.setExpireOwnSignInterval(request.getParameter("expireOwnSignInterval")==null?0: Integer.parseInt(request.getParameter("threadNumber")));
			taskType.setTaskParameter("");
			
			ScheduleStrategy scheduleStrategy = new ScheduleStrategy();
			scheduleStrategy.setStrategyName("strategyName");
			scheduleStrategy.setKind(ScheduleStrategy.Kind.Schedule);
			scheduleStrategy.setTaskName("taskName");
			scheduleStrategy.setOwnSign("ownSign");
			scheduleStrategy.setPermitRunEndTime("");
			scheduleStrategy.setPermitRunStartTime("");
			scheduleStrategy.setExecuteStrategy("0 0/1 * * * ?");
			scheduleStrategy.setExecuteType(ScheduleStrategy.Type.CronExp);
			
			scheduleStrategy.setNumOfSingleServer(1);
			scheduleStrategy.setAssignNum(1);
			scheduleStrategy.setIPList(null);
			scheduleStrategy.setSts("pause");
			createScheduleJob(taskType, scheduleStrategy);*/
			
			JobDealClient client = new JobDealClient();
			client.pause("test");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

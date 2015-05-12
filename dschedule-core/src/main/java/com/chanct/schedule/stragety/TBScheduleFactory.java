package com.chanct.schedule.stragety;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chanct.schedule.ConsoleManager;
import com.chanct.schedule.bean.IScheduleTaskDeal;
import com.chanct.schedule.datamanager.IScheduleDataManager;
import com.chanct.schedule.datamanager.IScheduleRuntimeManager;
import com.chanct.schedule.datamanager.ScheduleDataManager4ZK;
import com.chanct.schedule.datamanager.ScheduleRuntimeManager4Curator;
import com.chanct.schedule.entity.strategy.ManagerFactoryInfo;
import com.chanct.schedule.util.ScheduleUtil;
import com.chanct.schedule.zk.ScheduleFactoryDataManager4ZK;
import com.chanct.schedule.zk.ScheduleStrategyDataManager4ZK;
import com.chanct.schedule.zk.curator.CuratorManager;

/**
 * 调度服务器构造器
 * 
 * @author xuannan
 * 
 */
public class TBScheduleFactory  {

	protected static transient Logger logger = LoggerFactory.getLogger(TBScheduleFactory.class);

	private Map<String, String> zkConfig;

	private CuratorManager zkManager;

	private ManagerFactoryInfo	factoryInfo;
	/**
	 * 是否启动调度管理，如果只是做系统管理，应该设置为false
	 */
	public boolean start = true;

	/**
	 * 调度配置中心客服端
	 */
	private IScheduleDataManager scheduleDataManager;
	private ScheduleStrategyDataManager4ZK scheduleStrategyManager;
	private ScheduleFactoryDataManager4ZK scheduleFactoryManager;
	private  IScheduleRuntimeManager scheduleRuntimeManager ;
	private Map<String, List<IStrategyTask>> managerMap = new ConcurrentHashMap<String, List<IStrategyTask>>();


	private Timer timer;
	protected Lock lock = new ReentrantLock();

	volatile String errorMessage = "No config Zookeeper connect infomation";
	private InitialThread initialThread;

	public TBScheduleFactory() {
		factoryInfo = new ManagerFactoryInfo();
		factoryInfo.setIp(ScheduleUtil.getLocalIP());
		factoryInfo.setHostName(ScheduleUtil.getLocalHostName());
	}

	public void init() throws Exception {
		Properties properties = new Properties();
		for (Map.Entry<String, String> e : this.zkConfig.entrySet()) {
			properties.put(e.getKey(), e.getValue());
		}
		this.init(properties);
	}

	public void reInit(Properties p) throws Exception {
		if (this.start == true || this.timer != null || this.managerMap.size() > 0) {
			throw new Exception("调度器有任务处理，不能重新初始化");
		}
		this.init(p);
	}

	public void init(Properties p) throws Exception {
		if (this.initialThread != null) {
			this.initialThread.stopThread();
		}
		this.lock.lock();
		try {
			this.scheduleDataManager = null;
			this.scheduleStrategyManager = null;
			ConsoleManager.setScheduleManagerFactory(this);
			TBSchedule.setScheduleManagerFactory(this);
			if (this.zkManager != null) {
				this.zkManager.close();
			}
			this.zkManager = new CuratorManager(p);
			this.errorMessage = "Zookeeper connecting ......" + this.zkManager.getConnectStr();
			initialThread = new InitialThread(this);
			initialThread.setName("TBScheduleManagerFactory-initialThread");
			initialThread.start();
		} finally {
			this.lock.unlock();
		}
	}

	/**
	 * 在Zk状态正常后回调数据初始化
	 * 
	 * @throws Exception
	 */
	public void initialData() throws Exception {
		this.zkManager.initial();
		this.scheduleDataManager = new ScheduleDataManager4ZK(this.zkManager);
		this.scheduleStrategyManager = new ScheduleStrategyDataManager4ZK(this.zkManager);
		this.scheduleFactoryManager = new ScheduleFactoryDataManager4ZK(this.zkManager);
		this.scheduleRuntimeManager = new ScheduleRuntimeManager4Curator(zkManager);
		this.scheduleFactoryManager.registerManagerFactory(this.getFactoryInfo());
		/*if (this.start == true) {
			// 注册调度管理器
			this.scheduleStrategyManager.registerManagerFactory(this);
			if (timer == null) {
				timer = new Timer("TBScheduleManagerFactory-Timer");
			}
			if (timerTask == null) {
				timerTask = new ManagerFactoryTimerTask(this);
				timer.schedule(timerTask, 2000, this.timerInterval);
			}
		}*/
	}

	/**
	 * 创建调度服务器
	 * 
	 * @param baseTaskType
	 * @param ownSign
	 * @return
	 * @throws Exception
	 */
	/*
	public IStrategyTask createStrategyTask(ScheduleStrategy strategy) throws Exception {
		IStrategyTask result = null;
		if (ScheduleStrategy.Kind.Schedule == strategy.getKind()) {
			String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(strategy.getTaskName());
			String ownSign = ScheduleUtil.splitOwnsignFromTaskType(strategy.getTaskName());
			result = new TBScheduleManagerStatic(this, baseTaskType, ownSign, scheduleDataManager);
		} else if (ScheduleStrategy.Kind.Java == strategy.getKind()) {
			result = (IStrategyTask) Class.forName(strategy.getTaskName()).newInstance();
			result.initialTaskParameter(strategy.getStrategyName(), strategy.getTaskParameter());
		} else if (ScheduleStrategy.Kind.Bean == strategy.getKind()) {
			result = (IStrategyTask) this.getBean(strategy.getTaskName());
			result.initialTaskParameter(strategy.getStrategyName(), strategy.getTaskParameter());
		}
		return result;
	}

	public void refresh() throws Exception {
		this.lock.lock();
		try {
			// 判断状态是否终止
			ManagerFactoryInfo stsInfo = null;
			boolean isException = false;
			try {
				stsInfo = this.getScheduleStrategyManager().loadManagerFactoryInfo(this.getUuid());
			} catch (Exception e) {
				isException = true;
				logger.error(e.getMessage(), e);
			}
			if (isException == true) {
				try {
					stopServer(null); // 停止所有的调度任务
					this.getScheduleStrategyManager().unRregisterManagerFactory(this);
				} finally {
					reRegisterManagerFactory();
				}
			} else if (stsInfo.isStart() == false) {
				stopServer(null); // 停止所有的调度任务
				this.getScheduleStrategyManager().unRregisterManagerFactory(this);
			} else {
				reRegisterManagerFactory();
			}
		} finally {
			this.lock.unlock();
		}
	}
	public void reRegisterManagerFactory() throws Exception {
		// 重新分配调度器
		List<String> stopList = this.getScheduleStrategyManager().registerManagerFactory(this);
		for (String strategyName : stopList) {
			this.stopServer(strategyName);
		}
		this.assignScheduleServer();
		this.reRunScheduleServer();
	
	}*/
	/**
	 * 根据策略重新分配调度任务的机器
	 * 
	 * @throws Exception
	 */
	/*public void assignScheduleServer() throws Exception {
		for (ScheduleStrategyRunntime run : this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByUUID(this.uuid)) {
			// 遍历每个策略
			List<ScheduleStrategyRunntime> factoryList = this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByTaskType(run.getStrategyName());
			if (factoryList.size() == 0 || this.isLeader(this.uuid, factoryList) == false) {
				continue;
			}
			// 策略信息
			ScheduleStrategy scheduleStrategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
			// 分配任务，每个任务执行数
			int[] nums = ScheduleUtil.assignTaskNumber(factoryList.size(), scheduleStrategy.getAssignNum(), scheduleStrategy.getNumOfSingleServer());
			for (int i = 0; i < factoryList.size(); i++) {
				ScheduleStrategyRunntime factory = factoryList.get(i);
				// 更新请求的服务器数量
				this.scheduleStrategyManager.updateStrategyRunntimeReqestNum(run.getStrategyName(), factory.getUuid(), nums[i]);
			}
		}
	}

	public boolean isLeader(String uuid, List<ScheduleStrategyRunntime> factoryList) {
		try {
			long no = Long.parseLong(uuid.substring(uuid.lastIndexOf("$") + 1));
			for (ScheduleStrategyRunntime server : factoryList) {
				if (no > Long.parseLong(server.getUuid().substring(server.getUuid().lastIndexOf("$") + 1))) {
					return false;
				}
			}
			return true;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return true;
		}
	}

	public void reRunScheduleServer() throws Exception {
		List<ScheduleStrategyRunntime> runsList = this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByUUID(this.uuid);
		for (ScheduleStrategyRunntime run : runsList) {
			List<IStrategyTask> list = this.managerMap.get(run.getStrategyName());
			if (list == null) {
				list = new ArrayList<IStrategyTask>();
				this.managerMap.put(run.getStrategyName(), list);
			}
			// 多了，停止调度器
			while (list.size() > run.getRequestNum() && list.size() > 0) {
				IStrategyTask task = list.remove(list.size() - 1);
				try {
					task.stop(run.getStrategyName());
				} catch (Throwable e) {
					logger.error("注销任务错误：" + e.getMessage(), e);
				}
			}
			// 不足，增加调度器
			ScheduleStrategy strategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
			while (list.size() < run.getRequestNum()) {
				IStrategyTask result = this.createStrategyTask(strategy);
				list.add(result);
			}
		}
	}
*/
	/**
	 * 终止一类任务
	 * 
	 * @param taskType
	 * @throws Exception
	 */
	/*
	 * public void stopServer(String strategyName) throws Exception {
		if (strategyName == null) {
			String[] nameList = (String[]) this.managerMap.keySet().toArray(new String[0]);
			for (String name : nameList) {
				for (IStrategyTask task : this.managerMap.get(name)) {
					try {
						task.stop(strategyName);
					} catch (Throwable e) {
						logger.error("注销任务错误：" + e.getMessage(), e);
					}
				}
				this.managerMap.remove(name);
			}
		} else {
			List<IStrategyTask> list = this.managerMap.get(strategyName);
			if (list != null) {
				for (IStrategyTask task : list) {
					try {
						task.stop(strategyName);
					} catch (Throwable e) {
						logger.error("注销任务错误：" + e.getMessage(), e);
					}
				}
				this.managerMap.remove(strategyName);
			}

		}
	}
	*/
	/**
	 * 停止所有调度资源
	 */
	/*public void stopAll() throws Exception {
		try {
			lock.lock();
			this.start = false;
			if (this.initialThread != null) {
				this.initialThread.stopThread();
			}
			if (this.timer != null) {
				if (this.timerTask != null) {
					this.timerTask.cancel();
					this.timerTask = null;
				}
				this.timer.cancel();
				this.timer = null;
			}
			this.stopServer(null);
			if (this.zkManager != null) {
				this.zkManager.close();
			}
			if (this.scheduleStrategyManager != null) {
				try {
					ZooKeeper zk = this.scheduleStrategyManager.getZooKeeper();
					if (zk != null) {
						zk.close();
					}
				} catch (Exception e) {
				}
			}
			this.uuid = null;
			logger.warn("停止服务成功！");
		} catch (Throwable e) {
			logger.error("停止服务失败：" + e.getMessage(), e);
		} finally {
			lock.unlock();
		}
	}*/
	/**
	 * 重启所有的服务
	 * 
	 * @throws Exception
	 */
	/*public void reStart() throws Exception {
		try {
			if (this.timer != null) {
				if (this.timerTask != null) {
					this.timerTask.cancel();
					this.timerTask = null;
				}
				this.timer.purge();
			}
			this.stopServer(null);
			if (this.zkManager != null) {
				this.zkManager.close();
			}
			this.uuid = null;
			this.init();
		} catch (Throwable e) {
			logger.error("重启服务失败：" + e.getMessage(), e);
		}
	}*/
	
	
	public boolean isZookeeperInitialSucess() throws Exception {
		return this.zkManager.checkZookeeperState();
	}

	public IScheduleDataManager getScheduleDataManager() {
		if (this.scheduleDataManager == null) {
			throw new RuntimeException(this.errorMessage);
		}
		return scheduleDataManager;
	}
	public ScheduleStrategyDataManager4ZK getScheduleStrategyManager() {
		if (this.scheduleStrategyManager == null) {
			throw new RuntimeException(this.errorMessage);
		}
		return scheduleStrategyManager;
	}
	
	public ScheduleFactoryDataManager4ZK getScheduleFactoryManager() {
		if (this.scheduleStrategyManager == null) {
			throw new RuntimeException(this.errorMessage);
		}
		return scheduleFactoryManager;
	}


	public void setStart(boolean isStart) {
		this.start = isStart;
	}

	public void setZkConfig(Map<String, String> zkConfig) {
		this.zkConfig = zkConfig;
	}
	public CuratorManager getZkManager() {
		return this.zkManager;
	}
	public Map<String, String> getZkConfig() {
		return zkConfig;
	}

	public IScheduleRuntimeManager getScheduleRuntimeManager() {
		return scheduleRuntimeManager;
	}

	public InitialThread getInitialThread() {
		return initialThread;
	}

	public ManagerFactoryInfo getFactoryInfo() {
		return factoryInfo;
	}

	public void setFactoryInfo(ManagerFactoryInfo factoryInfo) {
		this.factoryInfo = factoryInfo;
	}

	
	
}


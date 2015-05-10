package com.taobao.pamirs.schedule.strategy;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.ConsoleManager;
import com.taobao.pamirs.schedule.client.JobDealClient;
import com.taobao.pamirs.schedule.datamanager.IScheduleDataManager;
import com.taobao.pamirs.schedule.entity.strategy.ManagerFactoryInfo;
import com.taobao.pamirs.schedule.entity.strategy.ScheduleStrategy;
import com.taobao.pamirs.schedule.entity.strategy.ScheduleStrategyRunntime;
import com.taobao.pamirs.schedule.strategy.task.AssignFactoryTimerTask;
import com.taobao.pamirs.schedule.strategy.task.ManagerFactoryTimerTask;
import com.taobao.pamirs.schedule.taskmanager.TBScheduleManagerGroup;
import com.taobao.pamirs.schedule.taskmanager.TBScheduleManagerStatic;
import com.taobao.pamirs.schedule.util.ScheduleUtil;
import com.taobao.pamirs.schedule.zk.CuratorCacheWatcher;
import com.taobao.pamirs.schedule.zk.ManagerWatcher;
import com.taobao.pamirs.schedule.zk.ScheduleStrategyDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ScheduleWatcher;
import com.taobao.pamirs.schedule.zk.ZKManager;
import com.taobao.pamirs.schedule.zk.curator.CuratorManager;

/**
 * 
 * 
 * @author : lihx create date : 2014-11-17
 */
public class TBSchedule {
	protected static transient Logger logger = LoggerFactory.getLogger(TBSchedule.class);
//	记录本机执行的策略和对应的任务列表
	private Map<String, List<IStrategyTask>> managerMap = new ConcurrentHashMap<String, List<IStrategyTask>>();
	private ScheduleStrategyDataManager4ZK scheduleStrategyManager;
	volatile String errorMessage = "No config Zookeeper connect infomation";

	protected Lock lock = new ReentrantLock();
	private Timer timer;
	private ManagerFactoryTimerTask timerTask;
	private AssignFactoryTimerTask assignTask;
	TBScheduleFactory factory = null;
	protected CuratorManager zkManager;
	
	private boolean firstAssign = true;
	/**
	 * 是否启动调度管理，如果只是做系统管理，应该设置为false
	 */
	// public boolean start = true;
	private int timerInterval = 5000;

	private boolean factoryNumsChange = false;
	/**
	 * 创建单例
	 */
	private TBSchedule() {

	}
	private static TBSchedule manager = new TBSchedule();

	public static TBSchedule getManagerIntance() {
		return manager;
	}
	/**
	 * 初始化manager
	 * 
	 * @throws Exception
	 */
	public void initialData() throws Exception {
		if (this.factory == null) {
			factory = new TBScheduleFactory();
			factory.init(loadProperties());
			Thread.sleep(500);
			// factory.start = false;
		}
		scheduleStrategyManager = factory.getScheduleStrategyManager();
		this.factory.getScheduleFactoryManager().watchFactory(new CuratorCacheWatcher(this.factory.getZkManager(), this));
		this.factory.getScheduleStrategyManager().watchStrategy(new CuratorCacheWatcher(this.factory.getZkManager(), this));
		if (this.factory.start == true) {
			// 注册调度管理器，间隔时间刷新数据
			
			if (timer == null) {
				timer = new Timer("TBScheduleManagerFactory-Timer");
			}
			if (timerTask == null) {
				timerTask = new ManagerFactoryTimerTask(this);
				timer.schedule(timerTask, 2000, this.timerInterval);
			}
			if (assignTask == null) {
				assignTask = new AssignFactoryTimerTask(this);
				timer.schedule(assignTask, 0, this.timerInterval * 20);
			}
		}
	}
	/**
	 * 在factory和strategy目录设置监听事件
	 * 此事件只由leader watch，触发后重新分配任务
	 * @throws Exception 
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	public void watchEvent() throws Exception {
//		this.factory.getScheduleFactoryManager().watchFactory(new ManagerWatcher(this.factory.getZkManager(), this));
//		this.factory.getScheduleStrategyManager().watchStrategy(new ManagerWatcher(this.factory.getZkManager(), this));
	}
	public void refresh() throws Exception {
		this.lock.lock();
		try {
			// 判断状态是否终止
			ManagerFactoryInfo stsInfo = null;
			boolean isException = false;
			try {
				stsInfo = this.factory.getScheduleFactoryManager().loadManagerFactoryInfo(this.factory.getUuid());
			} catch (Exception e) {
				isException = true;
				logger.error(e.getMessage(), e);
			}
			if (factoryNumsChange) {
				factoryNumsChange = false;
				assignScheduleFactory();
			}
			if (isException == true) {
				try {
					stopServer(null); // 停止所有的调度任务
					this.factory.getScheduleStrategyManager().unRregisterStrategyFactory(factory);
				} finally {
					reRegisterManagerFactory();
				}
//				如果此调度器为终止状态，停止所有服务
			} else if (stsInfo.isStart() == false) {
				stopServer(null); // 停止所有的调度任务
				this.factory.getScheduleStrategyManager().unRregisterStrategyFactory(factory);
			} else {
				reRegisterManagerFactory();
			}
		} finally {
			this.lock.unlock();
		}
	}
	public void reRegisterManagerFactory() throws Exception {
		// 重新分配调度器
		
//		停止当前不在本机运行的策略
		List<String> stopList = this.factory.getScheduleStrategyManager().loadAllNotScheduleStrategyRunntimeByUUID(factory.getUuid());
		for (String strategyName : stopList) {
			this.stopServer(strategyName);
		}
//		this.assignScheduleServer();
		this.reRunScheduleServer();
	}
	/**
	 * 根据策略重新分配调度任务的机器
	 * 只创建更新 strategy/factory的内容
	 * @throws Exception
	 */
	public void assignScheduleServer() throws Exception {
//		查找factory注册到的所有策略
		for (ScheduleStrategyRunntime run : this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByUUID(this.factory.getUuid())) {
			// 遍历每个策略，查找注册其下的所有factory
			List<ScheduleStrategyRunntime> factoryList = this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByTaskType(run.getStrategyName());
//			如果此策略下没有注册的factory，或者当前factory不是leader，直接返回不做后续处理
			if (factoryList.size() == 0 || this.isLeader(this.factory.getUuid(), factoryList) == false) {
				continue;
			}
			// 加载策略信息
			ScheduleStrategy scheduleStrategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
			// 计算每个factory任务执行数
//			TODO :**********************最终确实任务分配在哪个调度器上***********************
			
//			记录每台机器上的任务总数
			
			int[] nums = ScheduleUtil.assignTaskNumber(factoryList.size(), scheduleStrategy.getAssignNum(), scheduleStrategy.getNumOfSingleServer());
//			在strategy/factory里写入任务信息
			for (int i = 0; i < factoryList.size(); i++) {
				ScheduleStrategyRunntime factory = factoryList.get(i);
				// 更新请求的服务器数量
				this.scheduleStrategyManager.updateStrategyRunntimeReqestNum(run.getStrategyName(), factory.getUuid(), nums[i]);
			}
		}
	}
	
	/**
	 * 通过注册的factory来分配任务
	 * 启动、factory变化、strategy变化时候调用此方法
	 * @throws Exception
	 */
	public void assignScheduleFactory() throws Exception {
		logger.info(" into the assignScheduleFactory method!!!!!!!!!!!!!!!!!");
		this.lock.lock();
		try {
			// 清除不存在的机器所占的策略
			long start = System.currentTimeMillis();
			// 当前可用的所有的机器
			List<ManagerFactoryInfo> factoryList = this.factory.getScheduleFactoryManager().loadAllManagerFactoryInfo();
			List<String> factoryUUIDList = this.factory.getScheduleFactoryManager().loadAllFactory();
			if (this.factory.getScheduleFactoryManager().isLeader(this.factory.getUuid(), factoryUUIDList)) {
				// 如果当前有未被认领的策略，则之前的任务及所在机器不变，
				// 查找所有机器及其所认领的策略数
				// 查找所有未被认领的策略

				// 当前可用的但未使用的机器
				List<String> emptyFactoryList = new ArrayList<String>();

				// 记录所有机器的执行策略数
				Map<String, Integer> factoryNums = new HashMap<String, Integer>();

				for (ManagerFactoryInfo info : factoryList) {
					factoryNums.put(info.getUuid(), info.getRuns());
					if (info.getRuns() == 0) {
						emptyFactoryList.add(info.getUuid());
					}
				}
				// 所有的策略列表
				List<ScheduleStrategy> strategyList = this.factory.getScheduleStrategyManager().loadAllScheduleStrategy();
				// 循环策略，能算出没有被认领的策略及机器的执行策略数
				for (ScheduleStrategy strategy : strategyList) {
					List<ScheduleStrategyRunntime> list = this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByTaskType(strategy.getStrategyName());
					if (ScheduleStrategy.STS_PAUSE.equalsIgnoreCase(strategy.getSts())) {
						continue;
					}
					// 如果策略未被认领，直接分配
					if (list.isEmpty()) {
						logger.info("strategy: " + strategy.getStrategyName() + " is empty! Assign it!");
						assignFactoryForStrategy(strategy, factoryNums,strategy.getAssignNum());
						// emptyStrategyList.add(strategy);
					} else {
//						如果策略被分配执行，检查是否达到需求，不满足再分配其它未分配的机器
						int runNum = 0;
						// serverCloneList内保留当前策略中没有的机器id，从全部机器列表中删除已存在策略中执行的机器
						List<String> serverCloneList = new ArrayList<String>(factoryUUIDList);
						for (Iterator<ScheduleStrategyRunntime> iterator = list.iterator(); iterator.hasNext();) {
							ScheduleStrategyRunntime run = iterator.next();
							// 此run.uuid为存活的机器
							if (factoryNums.containsKey(run.getUuid())) {
//								如果当前机器可以运行此策略
								if (this.factory.getScheduleFactoryManager().checkFactoryCanRun(strategy,run.getUuid())){
									runNum += run.getRequestNum();									
								}else {
//									不能在本机运行，停止
									this.scheduleStrategyManager.updateStrategyRunntimeReqestNum(strategy.getStrategyName(), run.getUuid(),0);
								}
							} else {
								// 机器列表不包括策略下的uuid，删除
								System.out.println(strategy.getStrategyName() + "====" + run.getUuid());
								this.scheduleStrategyManager.clearExpireStrategyFactory(strategy.getStrategyName(), run.getUuid());
								iterator.remove();
							}
//							删掉当前已运行着此策略的机器 
							serverCloneList.remove(run.getUuid());
						}
//						删除不能运行策略的机器
						for (Iterator<String> iterator = serverCloneList.iterator(); iterator.hasNext();) {
							if (!this.factory.getScheduleFactoryManager().checkFactoryCanRun(strategy, iterator.next())) {
								iterator.remove();
							}
						}
						// 如果当前已运行的机器数及数量未满足要求，增加
						if (strategy.getAssignNum() > runNum && !serverCloneList.isEmpty()) {
							int[] nums = ScheduleUtil.assignTaskNumber(serverCloneList.size(), strategy.getAssignNum() - runNum, strategy.getNumOfSingleServer());
							for (int i = 0; i < serverCloneList.size(); i++) {
								String uuid = serverCloneList.get(i);
								// 更新请求的服务器数量
								System.out.println(strategy.getStrategyName() + "-----|-----" + uuid + "-----|----" + nums[i]);
								this.scheduleStrategyManager.updateStrategyRunntimeReqestNum(strategy.getStrategyName(), uuid, nums[i]);
								if (factoryNums.containsKey(uuid)) {
									factoryNums.put(uuid, factoryNums.get(uuid) + nums[i]);
								} else {
									factoryNums.put(uuid, nums[i]);
								}
							}
						}
					}

				}
				// 如果有闲置的机器并且没有闲置策略,或者相差很大，则将所有任务重新分配
				
				if (!emptyFactoryList.isEmpty() && !firstAssign) {
//					记录当前策略任务总数
					int taskNums = 0;

					for (Map.Entry<String, Integer> entry : factoryNums.entrySet()) {
						taskNums += entry.getValue();
					}
					if(taskNums<=factoryNums.size()) {
						return ;
					}
//					每台机器分配的任务数
					int[] nums = ScheduleUtil.assignTaskNumber(factoryNums.size(), taskNums, Integer.MAX_VALUE);
//					遍历出要释放的runtime的机器
					int index = 0;
					List<ScheduleStrategyRunntime> clearList = new ArrayList<ScheduleStrategyRunntime>();
					for (Map.Entry<String, Integer> entry : factoryNums.entrySet()) {
//						实际拥有策略数大于分摊后的数，有可分配
//						*****问题：任务组的如何分配。只能在当前机器执行的任务分配走了怎么办
						int clearNum =entry.getValue() - nums[index] ;
						if (clearNum > 0) {
							List<ScheduleStrategyRunntime> runsList = this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByUUID(entry.getKey());
							for (ScheduleStrategyRunntime run : runsList) {
//								足够分配
								if (run.getRequestNum() <= clearNum) {
//									判断当前执行条件，在其它机器上是否可执行，比如已指定机器ip，如果只单ip，不重分配，多ip，则检查其它ip机器是否正常
									ScheduleStrategy strategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
									if (strategy.getIPList() == null) {
										clearList.add(run);
										clearNum -= run.getRequestNum();
									} else if (strategy.getIPList().length == 1) {
//										只有一个时，不再分配
										continue;
									} else {
										for (String ip : strategy.getIPList()) {
											// 如果ip为当前执行机器，跳过
											if (run.getIp().equals(ip)) {
												continue;
											} else {
												// 如果可执行机器存在
												if (factoryNums.containsKey(ip)) {
													clearList.add(run);
													clearNum -= run.getRequestNum();
													break;
												}
											}
										}
									}
								}
								if (clearNum <= 0)
									break;
							}
						}
						index++;
					}
					
//					遍历出要释放的策略任务,挨个重分配，不再分配以前执行的机器上
					for (int i = 0; i < clearList.size(); i++) {
						logger.info("There has "+clearList.size()+" bean changed!");
						ScheduleStrategyRunntime old = clearList.get(i);
						ScheduleStrategy strategy = this.factory.getScheduleStrategyManager().loadStrategy(old.getStrategyName());
						assignFactoryForStrategy(strategy, factoryNums , old.getRequestNum());
						this.scheduleStrategyManager.updateStrategyRunntimeReqestNum(strategy.getStrategyName(), old.getUuid(),0);
//						this.scheduleStrategyManager.clearExpireStrategyFactory(strategy.getStrategyName(), old.getUuid());
					}
				}
				logger.info("*** use time :" + (System.currentTimeMillis() - start));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			this.lock.unlock();
			firstAssign = false;
		}
	}
	
	/**
	 * 为策略分配执行机器
	 * @param strategy
	 * @param factoryNums
	 * @param assignNum 本次要分配 的个数
	 * @throws Exception
	 */
	private void assignFactoryForStrategy(ScheduleStrategy strategy, Map<String, Integer> factoryNums, int assignNum) throws Exception {
		// 遍历出当前策略可执行的机器和机器上可运行此策略的最大个数
		Map<String, Integer> serverMap = new HashMap<String, Integer>();
		for (Map.Entry<String, Integer> entry : factoryNums.entrySet()) {
			if (this.factory.getScheduleFactoryManager().checkFactoryCanRun(strategy, entry.getKey())) {
				// 判断当前策略在此机器上是否达 到最大执行限制
				ScheduleStrategyRunntime run = this.scheduleStrategyManager.loadScheduleStrategyRunntime(strategy.getStrategyName(), entry.getKey());
				if ((run != null && run.getRequestNum() < strategy.getNumOfSingleServer())) {
					// 如果要分配的个数比此机器可以运行的个数小，直接分配
					if (assignNum <= (strategy.getNumOfSingleServer() - run.getRequestNum())) {
						this.scheduleStrategyManager.updateStrategyRunntimeReqestNum(strategy.getStrategyName(), entry.getKey(), assignNum);
					}
					serverMap.put(entry.getKey(), strategy.getNumOfSingleServer() - run.getRequestNum());
				} else if (run == null) {
					serverMap.put(entry.getKey(), strategy.getNumOfSingleServer());
				}
			}
		}
//		按每台机器可以再分的任务数，本次要分配的总数，每台机器最大执行数，来计算每台机器可以对此策略再分配多少任务数
		Map<String, Integer> resultNums = ScheduleUtil.assignTaskNumber(serverMap, assignNum, strategy.getNumOfSingleServer());
		// 在strategy/factory里写入任务信息
		for (Map.Entry<String, Integer> entry : resultNums.entrySet()) {
			factoryNums.put(entry.getKey(), factoryNums.get(entry.getKey()) + entry.getValue());
			System.out.println(strategy.getStrategyName() + "----------" + entry.getKey() + "---------" + entry.getValue());
			// 更新请求的服务器数量
			// 有可能之前此策略在这台机器分配过，不能覆盖，不能超额
			ScheduleStrategyRunntime run = this.scheduleStrategyManager.loadScheduleStrategyRunntime(strategy.getStrategyName(), entry.getKey());
			int willNum = entry.getValue();
			if (run != null) {
				willNum += run.getRequestNum();
			}
			this.scheduleStrategyManager.updateStrategyRunntimeReqestNum(strategy.getStrategyName(), entry.getKey(), willNum);
		}
	}
	
	/**
	 * 重新分配执行任务
	 * @throws Exception
	 */
	public void reRunScheduleServer() throws Exception {
		
		ManagerFactoryInfo info = this.factory.getScheduleFactoryManager().loadManagerFactoryInfo(this.factory.getUuid());
		int runs = 0;
//		查找当前factory注册到的所有策略
		List<ScheduleStrategyRunntime> runsList = this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByUUID(this.factory.getUuid());
		for (ScheduleStrategyRunntime run : runsList) {
			runs += run.getRequestNum();
			// 记录当前持有任务的信息
			List<IStrategyTask> list = this.managerMap.get(run.getStrategyName());
			if (list == null) {
				list = new ArrayList<IStrategyTask>();
				this.managerMap.put(run.getStrategyName(), list);
			}
			
			ScheduleStrategy strategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
//			如果当前策略已经停止，释放资源
			if (ScheduleStrategy.STS_PAUSE.equalsIgnoreCase(strategy.getSts())) {
				while (list.size() > 0) {
					IStrategyTask task = list.remove(list.size() - 1);
					try {
						System.out.println("stop " + run.getStrategyName());
						task.stop(run.getStrategyName());
					} catch (Throwable e) {
						logger.error("注销任务错误：" + e.getMessage(), e);
					}
				}
				this.scheduleStrategyManager.clearExpireStrategyFactory(strategy.getStrategyName(), run.getUuid());
				continue;
			}
			// 多了，停止调度器
			while (list.size() > run.getRequestNum() && list.size() > 0) {
				IStrategyTask task = list.remove(list.size() - 1);
				try {
					System.out.println("stop " + run.getStrategyName());
					task.stop(run.getStrategyName());
					if(list.isEmpty()) {
						this.scheduleStrategyManager.clearExpireStrategyFactory(strategy.getStrategyName(), run.getUuid());
					}
				} catch (Throwable e) {
					logger.error("注销任务错误：" + e.getMessage(), e);
				}
			}
			// 不足，增加调度器
			while (list.size() < run.getRequestNum()) {
				IStrategyTask result = this.createStrategyTask(strategy);
				list.add(result);
			}
		}
//		任务数有变化，更新
		if (info.getRuns() != runs) {
			logger.warn("任务数量变化：" + info.getRuns() + "----->" + runs);
			info.setRuns(runs);
//			if (runs == info.getAssign())
//				info.setAssign(0);
			this.factory.getScheduleFactoryManager().updateManagerFactoryInfo(info);
		}
//		if (info.getRuns() > info.getAssign() && info.getAssign() > 0) {
//			// 要释放的策略任务数
//			int lastNum = info.getRuns() - info.getAssign();
//			logger.warn("需求释放本机的执行任务：" + info.getRuns() + "----->" + info.getAssign());
//			for (Map.Entry<String, List<IStrategyTask>> entry : managerMap.entrySet()) {
////				将可以在任务机器运行的任务分配走
//				List<IStrategyTask> list = entry.getValue();
//				if (list != null && list.size() > 0 && lastNum > list.size()) {
//					ScheduleStrategy strategy = this.scheduleStrategyManager.loadStrategy(entry.getKey());
//					if (strategy.getIPList() != null) {
//						List<String> ipList = Arrays.asList(strategy.getIPList());
//						if (!ipList.contains("127.0.0.1") && ipList.contains("localhost")) {
//							continue;
//						}
//					}
//					try {
//						task.stop(run.getStrategyName());
//						lastNum -=list.size();
//					} catch (Throwable e) {
//						logger.error("注销任务错误：" + e.getMessage(), e);
//					}
//				}
//			}
//		}
	}
	
	/**
	 * 创建调度服务器
	 * 
	 * @param baseTaskType
	 * @param ownSign
	 * @return
	 * @throws Exception
	 */
	public IStrategyTask createStrategyTask(ScheduleStrategy strategy) throws Exception {
		IStrategyTask result = null;
		if (ScheduleStrategy.Kind.Schedule == strategy.getKind()) {
			String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(strategy.getTaskName());
			String ownSign = ScheduleUtil.splitOwnsignFromTaskType(strategy.getTaskName());
			result = new TBScheduleManagerStatic(factory, baseTaskType, ownSign, factory.getScheduleDataManager());
		} else if (ScheduleStrategy.Kind.Java == strategy.getKind()) {
			result = (IStrategyTask) Class.forName(strategy.getTaskName()).newInstance();
			result.initialTaskParameter(strategy.getStrategyName(), strategy.getTaskParameter());
		} else if (ScheduleStrategy.Kind.Bean == strategy.getKind()) {
			// TODO:未实现spring的bean
			// result = (IStrategyTask) this.getBean(strategy.getTaskName());
			// result.initialTaskParameter(strategy.getStrategyName(), strategy.getTaskParameter());
		} else if (ScheduleStrategy.Kind.Group == strategy.getKind()) {
			String baseTaskType = ScheduleUtil.splitBaseTaskTypeFromTaskType(strategy.getTaskName());
			String ownSign = ScheduleUtil.splitOwnsignFromTaskType(strategy.getTaskName());
			result = new TBScheduleManagerGroup(factory, baseTaskType, ownSign, factory.getScheduleDataManager(),strategy.getTaskParameter(),factory.getScheduleRuntimeManager());
		}
		return result;
	}
	/**
	 * 终止一类任务
	 * 
	 * @param taskType
	 * @throws Exception
	 */
	public void stopServer(String strategyName) throws Exception {
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
				System.out.println("======stop strategyName:"+strategyName);
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
	/**
	 * 停止所有调度资源
	 */
	public void stopAll() throws Exception {
		try {
			lock.lock();
			this.factory.start = false;
			if (this.factory.getInitialThread() != null) {
				this.factory.getInitialThread().stopThread();
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
			this.factory.setUuid(null);
			logger.warn("停止服务成功！");
		} catch (Throwable e) {
			logger.error("停止服务失败：" + e.getMessage(), e);
		} finally {
			lock.unlock();
		}
	}
	/**
	 * 重启所有的服务
	 * 
	 * @throws Exception
	 */
	public void reStart() throws Exception {
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
			this.factory.setUuid(null);
			factory.init();
		} catch (Throwable e) {
			logger.error("重启服务失败：" + e.getMessage(), e);
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
	private Properties loadProperties() throws Exception {
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
		return p;
	}
	public static void setScheduleManagerFactory(TBScheduleFactory scheduleManagerFactory) {
		getManagerIntance().factory = scheduleManagerFactory;
	}
	public void setTimerInterval(int timerInterval) {
		this.timerInterval = timerInterval;
	}
	public TBScheduleFactory getFactory() {
		return factory;
	}
	public boolean isFactoryNumsChange() {
		return factoryNumsChange;
	}
	public void setFactoryNumsChange(boolean factoryNumsChange) {
		this.factoryNumsChange = factoryNumsChange;
	}

}

class InitialThread extends Thread {
	private static transient Logger log = LoggerFactory.getLogger(InitialThread.class);
	TBScheduleFactory facotry;
	boolean isStop = false;
	public InitialThread(TBScheduleFactory aFactory) {
		this.facotry = aFactory;
	}
	public void stopThread() {
		this.isStop = true;
	}
	@Override
	public void run() {
		facotry.lock.lock();
		try {
			int count = 0;
			while (facotry.getZkManager().checkZookeeperState() == false) {
				count = count + 1;
				if (count % 50 == 0) {
					facotry.errorMessage = "Zookeeper connecting ......" + facotry.getZkManager().getConnectStr() + " spendTime:" + count * 20 + "(ms)";
					log.error(facotry.errorMessage);
				}
				Thread.sleep(20);
				if (this.isStop == true) {
					return;
				}
			}
			facotry.initialData();
		} catch (Throwable e) {
			log.error(e.getMessage(), e);
		} finally {
			facotry.lock.unlock();
		}

	}

}
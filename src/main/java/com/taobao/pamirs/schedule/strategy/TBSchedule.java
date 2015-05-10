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
//	��¼����ִ�еĲ��ԺͶ�Ӧ�������б�
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
	 * �Ƿ��������ȹ������ֻ����ϵͳ����Ӧ������Ϊfalse
	 */
	// public boolean start = true;
	private int timerInterval = 5000;

	private boolean factoryNumsChange = false;
	/**
	 * ��������
	 */
	private TBSchedule() {

	}
	private static TBSchedule manager = new TBSchedule();

	public static TBSchedule getManagerIntance() {
		return manager;
	}
	/**
	 * ��ʼ��manager
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
			// ע����ȹ����������ʱ��ˢ������
			
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
	 * ��factory��strategyĿ¼���ü����¼�
	 * ���¼�ֻ��leader watch�����������·�������
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
			// �ж�״̬�Ƿ���ֹ
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
					stopServer(null); // ֹͣ���еĵ�������
					this.factory.getScheduleStrategyManager().unRregisterStrategyFactory(factory);
				} finally {
					reRegisterManagerFactory();
				}
//				����˵�����Ϊ��ֹ״̬��ֹͣ���з���
			} else if (stsInfo.isStart() == false) {
				stopServer(null); // ֹͣ���еĵ�������
				this.factory.getScheduleStrategyManager().unRregisterStrategyFactory(factory);
			} else {
				reRegisterManagerFactory();
			}
		} finally {
			this.lock.unlock();
		}
	}
	public void reRegisterManagerFactory() throws Exception {
		// ���·��������
		
//		ֹͣ��ǰ���ڱ������еĲ���
		List<String> stopList = this.factory.getScheduleStrategyManager().loadAllNotScheduleStrategyRunntimeByUUID(factory.getUuid());
		for (String strategyName : stopList) {
			this.stopServer(strategyName);
		}
//		this.assignScheduleServer();
		this.reRunScheduleServer();
	}
	/**
	 * ���ݲ������·����������Ļ���
	 * ֻ�������� strategy/factory������
	 * @throws Exception
	 */
	public void assignScheduleServer() throws Exception {
//		����factoryע�ᵽ�����в���
		for (ScheduleStrategyRunntime run : this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByUUID(this.factory.getUuid())) {
			// ����ÿ�����ԣ�����ע�����µ�����factory
			List<ScheduleStrategyRunntime> factoryList = this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByTaskType(run.getStrategyName());
//			����˲�����û��ע���factory�����ߵ�ǰfactory����leader��ֱ�ӷ��ز�����������
			if (factoryList.size() == 0 || this.isLeader(this.factory.getUuid(), factoryList) == false) {
				continue;
			}
			// ���ز�����Ϣ
			ScheduleStrategy scheduleStrategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
			// ����ÿ��factory����ִ����
//			TODO :**********************����ȷʵ����������ĸ���������***********************
			
//			��¼ÿ̨�����ϵ���������
			
			int[] nums = ScheduleUtil.assignTaskNumber(factoryList.size(), scheduleStrategy.getAssignNum(), scheduleStrategy.getNumOfSingleServer());
//			��strategy/factory��д��������Ϣ
			for (int i = 0; i < factoryList.size(); i++) {
				ScheduleStrategyRunntime factory = factoryList.get(i);
				// ��������ķ���������
				this.scheduleStrategyManager.updateStrategyRunntimeReqestNum(run.getStrategyName(), factory.getUuid(), nums[i]);
			}
		}
	}
	
	/**
	 * ͨ��ע���factory����������
	 * ������factory�仯��strategy�仯ʱ����ô˷���
	 * @throws Exception
	 */
	public void assignScheduleFactory() throws Exception {
		logger.info(" into the assignScheduleFactory method!!!!!!!!!!!!!!!!!");
		this.lock.lock();
		try {
			// ��������ڵĻ�����ռ�Ĳ���
			long start = System.currentTimeMillis();
			// ��ǰ���õ����еĻ���
			List<ManagerFactoryInfo> factoryList = this.factory.getScheduleFactoryManager().loadAllManagerFactoryInfo();
			List<String> factoryUUIDList = this.factory.getScheduleFactoryManager().loadAllFactory();
			if (this.factory.getScheduleFactoryManager().isLeader(this.factory.getUuid(), factoryUUIDList)) {
				// �����ǰ��δ������Ĳ��ԣ���֮ǰ���������ڻ������䣬
				// �������л�������������Ĳ�����
				// ��������δ������Ĳ���

				// ��ǰ���õĵ�δʹ�õĻ���
				List<String> emptyFactoryList = new ArrayList<String>();

				// ��¼���л�����ִ�в�����
				Map<String, Integer> factoryNums = new HashMap<String, Integer>();

				for (ManagerFactoryInfo info : factoryList) {
					factoryNums.put(info.getUuid(), info.getRuns());
					if (info.getRuns() == 0) {
						emptyFactoryList.add(info.getUuid());
					}
				}
				// ���еĲ����б�
				List<ScheduleStrategy> strategyList = this.factory.getScheduleStrategyManager().loadAllScheduleStrategy();
				// ѭ�����ԣ������û�б�����Ĳ��Լ�������ִ�в�����
				for (ScheduleStrategy strategy : strategyList) {
					List<ScheduleStrategyRunntime> list = this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByTaskType(strategy.getStrategyName());
					if (ScheduleStrategy.STS_PAUSE.equalsIgnoreCase(strategy.getSts())) {
						continue;
					}
					// �������δ�����죬ֱ�ӷ���
					if (list.isEmpty()) {
						logger.info("strategy: " + strategy.getStrategyName() + " is empty! Assign it!");
						assignFactoryForStrategy(strategy, factoryNums,strategy.getAssignNum());
						// emptyStrategyList.add(strategy);
					} else {
//						������Ա�����ִ�У�����Ƿ�ﵽ���󣬲������ٷ�������δ����Ļ���
						int runNum = 0;
						// serverCloneList�ڱ�����ǰ������û�еĻ���id����ȫ�������б���ɾ���Ѵ��ڲ�����ִ�еĻ���
						List<String> serverCloneList = new ArrayList<String>(factoryUUIDList);
						for (Iterator<ScheduleStrategyRunntime> iterator = list.iterator(); iterator.hasNext();) {
							ScheduleStrategyRunntime run = iterator.next();
							// ��run.uuidΪ���Ļ���
							if (factoryNums.containsKey(run.getUuid())) {
//								�����ǰ�����������д˲���
								if (this.factory.getScheduleFactoryManager().checkFactoryCanRun(strategy,run.getUuid())){
									runNum += run.getRequestNum();									
								}else {
//									�����ڱ������У�ֹͣ
									this.scheduleStrategyManager.updateStrategyRunntimeReqestNum(strategy.getStrategyName(), run.getUuid(),0);
								}
							} else {
								// �����б����������µ�uuid��ɾ��
								System.out.println(strategy.getStrategyName() + "====" + run.getUuid());
								this.scheduleStrategyManager.clearExpireStrategyFactory(strategy.getStrategyName(), run.getUuid());
								iterator.remove();
							}
//							ɾ����ǰ�������Ŵ˲��ԵĻ��� 
							serverCloneList.remove(run.getUuid());
						}
//						ɾ���������в��ԵĻ���
						for (Iterator<String> iterator = serverCloneList.iterator(); iterator.hasNext();) {
							if (!this.factory.getScheduleFactoryManager().checkFactoryCanRun(strategy, iterator.next())) {
								iterator.remove();
							}
						}
						// �����ǰ�����еĻ�����������δ����Ҫ������
						if (strategy.getAssignNum() > runNum && !serverCloneList.isEmpty()) {
							int[] nums = ScheduleUtil.assignTaskNumber(serverCloneList.size(), strategy.getAssignNum() - runNum, strategy.getNumOfSingleServer());
							for (int i = 0; i < serverCloneList.size(); i++) {
								String uuid = serverCloneList.get(i);
								// ��������ķ���������
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
				// ��������õĻ�������û�����ò���,�������ܴ��������������·���
				
				if (!emptyFactoryList.isEmpty() && !firstAssign) {
//					��¼��ǰ������������
					int taskNums = 0;

					for (Map.Entry<String, Integer> entry : factoryNums.entrySet()) {
						taskNums += entry.getValue();
					}
					if(taskNums<=factoryNums.size()) {
						return ;
					}
//					ÿ̨���������������
					int[] nums = ScheduleUtil.assignTaskNumber(factoryNums.size(), taskNums, Integer.MAX_VALUE);
//					������Ҫ�ͷŵ�runtime�Ļ���
					int index = 0;
					List<ScheduleStrategyRunntime> clearList = new ArrayList<ScheduleStrategyRunntime>();
					for (Map.Entry<String, Integer> entry : factoryNums.entrySet()) {
//						ʵ��ӵ�в��������ڷ�̯��������пɷ���
//						*****���⣺���������η��䡣ֻ���ڵ�ǰ����ִ�е��������������ô��
						int clearNum =entry.getValue() - nums[index] ;
						if (clearNum > 0) {
							List<ScheduleStrategyRunntime> runsList = this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByUUID(entry.getKey());
							for (ScheduleStrategyRunntime run : runsList) {
//								�㹻����
								if (run.getRequestNum() <= clearNum) {
//									�жϵ�ǰִ���������������������Ƿ��ִ�У�������ָ������ip�����ֻ��ip�����ط��䣬��ip����������ip�����Ƿ�����
									ScheduleStrategy strategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
									if (strategy.getIPList() == null) {
										clearList.add(run);
										clearNum -= run.getRequestNum();
									} else if (strategy.getIPList().length == 1) {
//										ֻ��һ��ʱ�����ٷ���
										continue;
									} else {
										for (String ip : strategy.getIPList()) {
											// ���ipΪ��ǰִ�л���������
											if (run.getIp().equals(ip)) {
												continue;
											} else {
												// �����ִ�л�������
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
					
//					������Ҫ�ͷŵĲ�������,�����ط��䣬���ٷ�����ǰִ�еĻ�����
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
	 * Ϊ���Է���ִ�л���
	 * @param strategy
	 * @param factoryNums
	 * @param assignNum ����Ҫ���� �ĸ���
	 * @throws Exception
	 */
	private void assignFactoryForStrategy(ScheduleStrategy strategy, Map<String, Integer> factoryNums, int assignNum) throws Exception {
		// ��������ǰ���Կ�ִ�еĻ����ͻ����Ͽ����д˲��Ե�������
		Map<String, Integer> serverMap = new HashMap<String, Integer>();
		for (Map.Entry<String, Integer> entry : factoryNums.entrySet()) {
			if (this.factory.getScheduleFactoryManager().checkFactoryCanRun(strategy, entry.getKey())) {
				// �жϵ�ǰ�����ڴ˻������Ƿ�� �����ִ������
				ScheduleStrategyRunntime run = this.scheduleStrategyManager.loadScheduleStrategyRunntime(strategy.getStrategyName(), entry.getKey());
				if ((run != null && run.getRequestNum() < strategy.getNumOfSingleServer())) {
					// ���Ҫ����ĸ����ȴ˻����������еĸ���С��ֱ�ӷ���
					if (assignNum <= (strategy.getNumOfSingleServer() - run.getRequestNum())) {
						this.scheduleStrategyManager.updateStrategyRunntimeReqestNum(strategy.getStrategyName(), entry.getKey(), assignNum);
					}
					serverMap.put(entry.getKey(), strategy.getNumOfSingleServer() - run.getRequestNum());
				} else if (run == null) {
					serverMap.put(entry.getKey(), strategy.getNumOfSingleServer());
				}
			}
		}
//		��ÿ̨���������ٷֵ�������������Ҫ�����������ÿ̨�������ִ������������ÿ̨�������ԶԴ˲����ٷ������������
		Map<String, Integer> resultNums = ScheduleUtil.assignTaskNumber(serverMap, assignNum, strategy.getNumOfSingleServer());
		// ��strategy/factory��д��������Ϣ
		for (Map.Entry<String, Integer> entry : resultNums.entrySet()) {
			factoryNums.put(entry.getKey(), factoryNums.get(entry.getKey()) + entry.getValue());
			System.out.println(strategy.getStrategyName() + "----------" + entry.getKey() + "---------" + entry.getValue());
			// ��������ķ���������
			// �п���֮ǰ�˲�������̨��������������ܸ��ǣ����ܳ���
			ScheduleStrategyRunntime run = this.scheduleStrategyManager.loadScheduleStrategyRunntime(strategy.getStrategyName(), entry.getKey());
			int willNum = entry.getValue();
			if (run != null) {
				willNum += run.getRequestNum();
			}
			this.scheduleStrategyManager.updateStrategyRunntimeReqestNum(strategy.getStrategyName(), entry.getKey(), willNum);
		}
	}
	
	/**
	 * ���·���ִ������
	 * @throws Exception
	 */
	public void reRunScheduleServer() throws Exception {
		
		ManagerFactoryInfo info = this.factory.getScheduleFactoryManager().loadManagerFactoryInfo(this.factory.getUuid());
		int runs = 0;
//		���ҵ�ǰfactoryע�ᵽ�����в���
		List<ScheduleStrategyRunntime> runsList = this.scheduleStrategyManager.loadAllScheduleStrategyRunntimeByUUID(this.factory.getUuid());
		for (ScheduleStrategyRunntime run : runsList) {
			runs += run.getRequestNum();
			// ��¼��ǰ�����������Ϣ
			List<IStrategyTask> list = this.managerMap.get(run.getStrategyName());
			if (list == null) {
				list = new ArrayList<IStrategyTask>();
				this.managerMap.put(run.getStrategyName(), list);
			}
			
			ScheduleStrategy strategy = this.scheduleStrategyManager.loadStrategy(run.getStrategyName());
//			�����ǰ�����Ѿ�ֹͣ���ͷ���Դ
			if (ScheduleStrategy.STS_PAUSE.equalsIgnoreCase(strategy.getSts())) {
				while (list.size() > 0) {
					IStrategyTask task = list.remove(list.size() - 1);
					try {
						System.out.println("stop " + run.getStrategyName());
						task.stop(run.getStrategyName());
					} catch (Throwable e) {
						logger.error("ע���������" + e.getMessage(), e);
					}
				}
				this.scheduleStrategyManager.clearExpireStrategyFactory(strategy.getStrategyName(), run.getUuid());
				continue;
			}
			// ���ˣ�ֹͣ������
			while (list.size() > run.getRequestNum() && list.size() > 0) {
				IStrategyTask task = list.remove(list.size() - 1);
				try {
					System.out.println("stop " + run.getStrategyName());
					task.stop(run.getStrategyName());
					if(list.isEmpty()) {
						this.scheduleStrategyManager.clearExpireStrategyFactory(strategy.getStrategyName(), run.getUuid());
					}
				} catch (Throwable e) {
					logger.error("ע���������" + e.getMessage(), e);
				}
			}
			// ���㣬���ӵ�����
			while (list.size() < run.getRequestNum()) {
				IStrategyTask result = this.createStrategyTask(strategy);
				list.add(result);
			}
		}
//		�������б仯������
		if (info.getRuns() != runs) {
			logger.warn("���������仯��" + info.getRuns() + "----->" + runs);
			info.setRuns(runs);
//			if (runs == info.getAssign())
//				info.setAssign(0);
			this.factory.getScheduleFactoryManager().updateManagerFactoryInfo(info);
		}
//		if (info.getRuns() > info.getAssign() && info.getAssign() > 0) {
//			// Ҫ�ͷŵĲ���������
//			int lastNum = info.getRuns() - info.getAssign();
//			logger.warn("�����ͷű�����ִ������" + info.getRuns() + "----->" + info.getAssign());
//			for (Map.Entry<String, List<IStrategyTask>> entry : managerMap.entrySet()) {
////				������������������е����������
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
//						logger.error("ע���������" + e.getMessage(), e);
//					}
//				}
//			}
//		}
	}
	
	/**
	 * �������ȷ�����
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
			// TODO:δʵ��spring��bean
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
	 * ��ֹһ������
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
						logger.error("ע���������" + e.getMessage(), e);
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
						logger.error("ע���������" + e.getMessage(), e);
					}
				}
				this.managerMap.remove(strategyName);
			}

		}
	}
	/**
	 * ֹͣ���е�����Դ
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
			logger.warn("ֹͣ����ɹ���");
		} catch (Throwable e) {
			logger.error("ֹͣ����ʧ�ܣ�" + e.getMessage(), e);
		} finally {
			lock.unlock();
		}
	}
	/**
	 * �������еķ���
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
			logger.error("��������ʧ�ܣ�" + e.getMessage(), e);
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
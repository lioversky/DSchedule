package com.taobao.pamirs.schedule.zk;

import java.io.Writer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.taobao.pamirs.schedule.entity.strategy.ManagerFactoryInfo;
import com.taobao.pamirs.schedule.entity.strategy.ScheduleStrategy;
import com.taobao.pamirs.schedule.entity.strategy.ScheduleStrategyRunntime;
import com.taobao.pamirs.schedule.strategy.TBScheduleFactory;
import com.taobao.pamirs.schedule.util.TimestampTypeAdapter;
import com.taobao.pamirs.schedule.zk.curator.CuratorManager;

public class ScheduleStrategyDataManager4ZK{
	
	private CuratorManager zkManager;
	private String PATH_Strategy;
	private Gson gson ;
	
	//��Spring���󴴽���Ϻ󣬴����ڲ�����
    public ScheduleStrategyDataManager4ZK(CuratorManager aZkManager) throws Exception {
    	this.zkManager = aZkManager;
		gson = new GsonBuilder().registerTypeAdapter(Timestamp.class,new TimestampTypeAdapter()).setDateFormat("yyyy-MM-dd HH:mm:ss").create();		
		this.PATH_Strategy = this.zkManager.getRootPath() +  "/strategy";
		
		
		if (this.getZooKeeper().exists(this.PATH_Strategy, false) == null) {
			ZKTools.createPath(getZooKeeper(),this.PATH_Strategy, CreateMode.PERSISTENT, this.zkManager.getAcl());
		}
	}	

	public ScheduleStrategy loadStrategy(String strategyName)
			throws Exception {
		String zkPath = this.PATH_Strategy + "/" + strategyName;
		if(this.getZooKeeper().exists(zkPath, false) == null){
			return null;
		}
		String valueString= new String(this.getZooKeeper().getData(zkPath,false,null));
		ScheduleStrategy result = (ScheduleStrategy)this.gson.fromJson(valueString, ScheduleStrategy.class);
		return result;
	}
	
	public void createScheduleStrategy(ScheduleStrategy scheduleStrategy) throws Exception {
		String zkPath =	this.PATH_Strategy + "/"+ scheduleStrategy.getStrategyName();
		String valueString = this.gson.toJson(scheduleStrategy);
		if ( this.getZooKeeper().exists(zkPath, false) == null) {
			this.getZooKeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(),CreateMode.PERSISTENT);
		} else {
			throw new Exception("���Ȳ���" + scheduleStrategy.getStrategyName() + "�Ѿ�����,���ȷ����Ҫ�ؽ������ȵ���deleteMachineStrategy(String taskType)ɾ��");
		}
	}

	public void updateScheduleStrategy(ScheduleStrategy scheduleStrategy)
			throws Exception {
		String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName();
		String valueString = this.gson.toJson(scheduleStrategy);
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			this.getZooKeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(),CreateMode.PERSISTENT);
		} else {
			this.getZooKeeper().setData(zkPath, valueString.getBytes(), -1);
		}

	}
	public void deleteMachineStrategy(String taskType) throws Exception {
		deleteMachineStrategy(taskType,false);
	}
    public void pause(String strategyName) throws Exception{
    	ScheduleStrategy strategy = this.loadStrategy(strategyName);
    	strategy.setSts(ScheduleStrategy.STS_PAUSE);
    	this.updateScheduleStrategy(strategy);
	}
	public void resume(String strategyName) throws Exception{
    	ScheduleStrategy strategy = this.loadStrategy(strategyName);
    	strategy.setSts(ScheduleStrategy.STS_RESUME);
    	this.updateScheduleStrategy(strategy);		
	}
	
	public void deleteMachineStrategy(String taskType,boolean isForce) throws Exception {
		String zkPath = this.PATH_Strategy + "/" + taskType;
		if(isForce == false && this.getZooKeeper().getChildren(zkPath,null).size() >0){
			throw new Exception("����ɾ��"+ taskType +"�����в��ԣ��ᵼ�±�����������Ӧ�ò���ֹͣʧȥ���Ƶĵ��Ƚ��̡�" +
					"���������IP��ַ�������еĵ�������ֹͣ����ɾ�����Ȳ���");
		}
		ZKTools.deleteTree(this.getZooKeeper(),zkPath);
	}

	/**
	 * �������еĵ��Ȳ���
	 * @return
	 * @throws Exception
	 */
	public List<ScheduleStrategy> loadAllScheduleStrategy() throws Exception {
		String zkPath = this.PATH_Strategy;
		List<ScheduleStrategy> result = new ArrayList<ScheduleStrategy>();
		List<String> names = this.getZooKeeper().getChildren(zkPath,false);
		Collections.sort(names);
		for(String name:names){
			result.add(this.loadStrategy(name));
		}
		return result;
	}
	
	/**
	 * �����˷�������ִ�еĲ��ԣ������ִ�еĲ���
	 * @param managerFactory
	 * @return
	 * @throws Exception
	 */
	public List<String> reloadStrategyFactory(TBScheduleFactory managerFactory) throws Exception {
		// ��ͣ���߲���IP��������ִ�з�Χ�ڵ�ִ�в���
		List<String> result = new ArrayList<String>();
		for (ScheduleStrategy scheduleStrategy : loadAllScheduleStrategy()) {
			boolean isFind = false;
			// �ڱ�������IP��Χ�ڵ�ִ�в��ԣ�ע��ڵ�
			if (ScheduleStrategy.STS_PAUSE.equalsIgnoreCase(scheduleStrategy.getSts()) == false && scheduleStrategy.getIPList() != null) {
				for (String ip : scheduleStrategy.getIPList()) {
					if (ip.equals("127.0.0.1") || ip.equalsIgnoreCase("localhost") || ip.equals(managerFactory.getIp()) || ip.equalsIgnoreCase(managerFactory.getHostName())) {
						// ��ӿɹ���TaskType
						String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName() + "/" + managerFactory.getUuid();
						if (this.getZooKeeper().exists(zkPath, false) == null) {
							zkPath = this.getZooKeeper().create(zkPath, null, this.zkManager.getAcl(), CreateMode.EPHEMERAL);
						}
						isFind = true;
						break;
					}
				}
			}
			// ���ԭ��ע���,���ڲ��ڴ˷�������Factory
			if (isFind == false) {
				String zkPath = this.PATH_Strategy + "/" + scheduleStrategy.getStrategyName() + "/" + managerFactory.getUuid();
				if (this.getZooKeeper().exists(zkPath, false) != null) {
					ZKTools.deleteTree(this.getZooKeeper(), zkPath);
					result.add(scheduleStrategy.getStrategyName());
				}
			}
		}
		return result;
	}
	/**
	 * ע������ֹͣ����
	 * @param managerFactory
	 * @return
	 * @throws Exception
	 */
	public void unRregisterStrategyFactory(TBScheduleFactory managerFactory) throws Exception{
		System.out.println(22222);
		for(String taskName:this.getZooKeeper().getChildren(this.PATH_Strategy,false)){
			String zkPath =	this.PATH_Strategy+"/"+taskName+"/" + managerFactory.getUuid();
			if(this.getZooKeeper().exists(zkPath, false)!=null){
				ZKTools.deleteTree(this.getZooKeeper(), zkPath);
			}
		}
	}
	/**
	 * strategyName�����µ�uuidִ����Ϣ
	 * @param strategyName
	 * @param uuid
	 * @return
	 * @throws Exception
	 */
	public ScheduleStrategyRunntime loadScheduleStrategyRunntime(String strategyName,String uuid) throws Exception{
		String zkPath =	this.PATH_Strategy +"/"+strategyName+"/"+uuid;
		ScheduleStrategyRunntime result = null;
		if(this.getZooKeeper().exists(zkPath, false) !=null){
			byte[] value = this.getZooKeeper().getData(zkPath,false,null);
			if (value != null) {
				String valueString = new String(this.getZooKeeper().getData(zkPath,false,null));
				result = (ScheduleStrategyRunntime) this.gson.fromJson(valueString, ScheduleStrategyRunntime.class);
			}else{
				result = new ScheduleStrategyRunntime();
				result.setStrategyName(strategyName);
				result.setUuid(uuid);
				result.setRequestNum(0);
				result.setMessage("");
			}
		}
		return result;
	}
	
	/**
	 * װ�����еĲ�������״̬
	 * @return
	 * @throws Exception
	 */
	public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntime() throws Exception{
		List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
		String zkPath =	this.PATH_Strategy;
		for(String taskType: this.getZooKeeper().getChildren(zkPath, false)){
			for(String uuid:this.getZooKeeper().getChildren(zkPath+"/"+taskType,false)){
				result.add(loadScheduleStrategyRunntime(taskType,uuid));
			}
		}
		return result;
	}
	/**
	 * ����managerFactoryUUID��ѯ���в����µ�ִ�е�����
	 * @param managerFactoryUUID
	 * @return
	 * @throws Exception
	 */
	public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntimeByUUID(String managerFactoryUUID) throws Exception{
		List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
		String zkPath =	this.PATH_Strategy;
//		�����µ����нڵ㣨�������ͣ�
		List<String> taskTypeList =  this.getZooKeeper().getChildren(zkPath, false);
		Collections.sort(taskTypeList);		
		for(String taskType:taskTypeList){
			if(this.getZooKeeper().exists(zkPath+"/"+taskType+"/"+managerFactoryUUID, false) !=null){				
				result.add(loadScheduleStrategyRunntime(taskType,managerFactoryUUID));
			}
		}
		return result;
	}
	/**
	 * ���������Ѳ��ڱ���ִ�еĲ���
	 * @param managerFactoryUUID
	 * @return
	 * @throws Exception
	 */
	public List<String> loadAllNotScheduleStrategyRunntimeByUUID(String managerFactoryUUID) throws Exception{
		List<String> result = new ArrayList<String>();
		String zkPath =	this.PATH_Strategy;
//		�����µ����нڵ㣨�������ͣ�
		List<String> taskTypeList =  this.getZooKeeper().getChildren(zkPath, false);
		Collections.sort(taskTypeList);		
		for(String taskType:taskTypeList){
			if(this.getZooKeeper().exists(zkPath+"/"+taskType+"/"+managerFactoryUUID, false) ==null){				
				result.add(taskType);
			}
		}
		return result;
	}
	/**
	 * ����strategyName�������������ִ�е���Ϣ
	 * @param strategyName
	 * @return
	 * @throws Exception
	 */
	public List<ScheduleStrategyRunntime> loadAllScheduleStrategyRunntimeByTaskType(String strategyName) throws Exception{
		List<ScheduleStrategyRunntime> result = new ArrayList<ScheduleStrategyRunntime>();
		String zkPath =	this.PATH_Strategy;
		if(this.getZooKeeper().exists(zkPath+"/"+strategyName, false)==null){
			return result;
		}
		List<String> uuidList = this.getZooKeeper().getChildren(zkPath + "/" + strategyName, false);
		//����
		Collections.sort(uuidList,new Comparator<String>(){
			public int compare(String u1, String u2) {
				return u1.substring(u1.lastIndexOf("$") + 1).compareTo(
						u2.substring(u2.lastIndexOf("$") + 1));
			}
		});
		
		for (String uuid :uuidList) {
			result.add(loadScheduleStrategyRunntime(strategyName,uuid));
		}
		return result;
	}
	/**
	 * ������������
	 * @param taskType
	 * @param manangerFactoryUUID
	 * @param requestNum
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void updateStrategyRunntimeReqestNum(String strategyName,String manangerFactoryUUID,int requestNum) throws Exception{
		String zkPath =	this.PATH_Strategy +"/"+strategyName+"/" + manangerFactoryUUID;
		ScheduleStrategyRunntime result = null;
		if(this.getZooKeeper().exists(zkPath, false) !=null){
			result = this.loadScheduleStrategyRunntime(strategyName,manangerFactoryUUID);
		} else {
			result = new ScheduleStrategyRunntime();
			result.setStrategyName(strategyName);
			result.setUuid(manangerFactoryUUID);
//			result.setRequestNum(requestNum);
			result.setMessage("");
		}
		result.setRequestNum(requestNum);
		String valueString = this.gson.toJson(result);	
		if (this.getZooKeeper().exists(zkPath, false) == null) {
			zkPath = this.getZooKeeper().create(zkPath, valueString.getBytes(), this.zkManager.getAcl(), CreateMode.EPHEMERAL);
		}else 
			this.getZooKeeper().setData(zkPath,valueString.getBytes(),-1);
	}
	/**
	 * ���µ��ȹ����е���Ϣ
	 * @param strategyName
	 * @param manangerFactoryUUID
	 * @param message
	 * @throws Exception
	 */
	public void updateStrategyRunntimeErrorMessage(String strategyName,String manangerFactoryUUID,String message) throws Exception{
		String zkPath =	this.PATH_Strategy +"/"+strategyName+"/" + manangerFactoryUUID;
		ScheduleStrategyRunntime result = null;
		if(this.getZooKeeper().exists(zkPath, false) !=null){
			result = this.loadScheduleStrategyRunntime(strategyName,manangerFactoryUUID);
		} else {
			result = new ScheduleStrategyRunntime();
			result.setStrategyName(strategyName);
			result.setUuid(manangerFactoryUUID);
			result.setRequestNum(0);
		}
		result.setMessage(message);
		String valueString = this.gson.toJson(result);	
		this.getZooKeeper().setData(zkPath,valueString.getBytes(),-1);
		}
	
	
	
	/**
	 * ����������Ϣ��Ŀǰ֧��baseTaskType��strategy���ݡ�
	 * 
	 * @param config
	 * @param writer
	 * @param isUpdate
	 * @throws Exception
	 */
	public void importConfig(String config, Writer writer, boolean isUpdate)
			throws Exception {
		ConfigNode configNode = gson.fromJson(config, ConfigNode.class);
		if (configNode != null) {
			String path = configNode.getRootPath() + "/"
					+ configNode.getConfigType();
			ZKTools.createPath(getZooKeeper(), path, CreateMode.PERSISTENT, zkManager.getAcl());
			String y_node = path + "/" + configNode.getName();
			if (getZooKeeper().exists(y_node, false) == null) {
				writer.append("<font color=\"red\">�ɹ�������������Ϣ\n</font>");
				getZooKeeper().create(y_node, configNode.getValue().getBytes(),
						zkManager.getAcl(), CreateMode.PERSISTENT);
			} else if (isUpdate) {
				writer.append("<font color=\"red\">��������Ϣ�Ѿ����ڣ�����ǿ�Ƹ�����\n</font>");
				getZooKeeper().setData(y_node,
						configNode.getValue().getBytes(), -1);
			} else {
				writer.append("<font color=\"red\">��������Ϣ�Ѿ����ڣ������Ҫ���£�������ǿ�Ƹ���\n</font>");
			}
		}
		writer.append(configNode.toString());
	}

	/**
	 * ���������Ϣ��Ŀǰ����baseTaskType��strategy���ݡ�
	 * 
	 * @param rootPath
	 * @param writer
	 * @throws Exception
	 */
	public StringBuffer exportConfig(String rootPath, Writer writer)
			throws Exception {
		StringBuffer buffer = new StringBuffer();
		for (String type : new String[] { "baseTaskType", "strategy" }) {
			if (type.equals("baseTaskType")) {
				writer.write("<h2>�������������б�</h2>\n");
			} else {
				writer.write("<h2>�������������б�</h2>\n");
			}
			String bTTypePath = rootPath + "/" + type;
			List<String> fNodeList = getZooKeeper().getChildren(bTTypePath,
					false);
			for (int i = 0; i < fNodeList.size(); i++) {
				String fNode = fNodeList.get(i);
				ConfigNode configNode = new ConfigNode(rootPath, type, fNode);
				configNode.setValue(new String(this.getZooKeeper().getData(bTTypePath + "/" + fNode,false,null)));
				buffer.append(gson.toJson(configNode));
				buffer.append("\n");
				writer.write(configNode.toString());
			}
			writer.write("\n\n");
		}
		if (buffer.length() > 0) {
			String str = buffer.toString();
			return new StringBuffer(str.substring(0, str.length() - 1));
		}
		return buffer;
	}
		
	public void clearExpireStrategyFactory(String strategyName, String factoryUuid) throws Exception {
		String zkPath = this.PATH_Strategy + "/" + strategyName + "/" + factoryUuid;
		if (this.getZooKeeper().exists(zkPath, false) != null) {
			this.getZooKeeper().delete(zkPath, -1);
		}
	}
	public void watchStrategy(final Watcher watcher) throws Exception {
		// this.getZooKeeper().getChildren(this.PATH_Strategy, watcher);

		// this.getZooKeeper().getChildren(this.PATH_ManagerFactory, watcher);
		PathChildrenCache cache = new PathChildrenCache(this.zkManager.getCurator(), this.PATH_Strategy, true);
		cache.start(StartMode.POST_INITIALIZED_EVENT);
		cache.getListenable().addListener(new PathChildrenCacheListener() {
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				switch (event.getType()) {
					case CHILD_ADDED :
						watcher.process(null);
						break;
					case CHILD_UPDATED :
						watcher.process(null);
						break;
					case CHILD_REMOVED :
						watcher.process(null);
						break;
					default :
						break;
				}
			}
		});

	}
	public void printTree(String path, Writer writer,String lineSplitChar)
			throws Exception {
		ZKTools.printTree(this.getZooKeeper(),path,writer,lineSplitChar);
	}
	public void deleteTree(String path) throws Exception{
		ZKTools.deleteTree(this.getZooKeeper(), path);
	}
	public ZooKeeper getZooKeeper() throws Exception {
		return this.zkManager.getZooKeeper();
	}
	public String getRootPath(){
		return this.zkManager.getRootPath();
	}
}

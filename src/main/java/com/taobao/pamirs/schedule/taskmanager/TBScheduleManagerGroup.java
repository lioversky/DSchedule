package com.taobao.pamirs.schedule.taskmanager;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.bean.IScheduleTaskDeal;
import com.taobao.pamirs.schedule.datamanager.IScheduleDataManager;
import com.taobao.pamirs.schedule.datamanager.IScheduleRuntimeManager;
import com.taobao.pamirs.schedule.entity.TaskItemDefine;
import com.taobao.pamirs.schedule.entity.manager.ScheduleServer;
import com.taobao.pamirs.schedule.strategy.TBScheduleFactory;
import com.taobao.pamirs.schedule.taskmanager.task.HeartBeatTimerTask;
import com.taobao.pamirs.schedule.util.CronExpression;
import com.taobao.pamirs.schedule.util.ScheduleUtil;

/**
 * ������ִ�п�����
 * 
 * ����������ͣ�������ͬ����ͬһ����
 * 
 * ��type�´���groupItem�����¼��ڵ�Ϊ��������
 * 
 * 
 * ��������ȵ���assign��������item
 * 
 * @author 
 *
 */
public class TBScheduleManagerGroup extends TBScheduleManager {
	private static transient Logger log = LoggerFactory.getLogger(TBScheduleManagerGroup.class);
    /**
	 * �ܵ���������
	 */
    protected int taskItemCount =0;

    protected long lastFetchVersion = -1;
    
    private final Object NeedReloadTaskItemLock = new Object();
    IScheduleRuntimeManager runtimeManager ;
    
	public TBScheduleManagerGroup(TBScheduleFactory aFactory, String baseTaskType, String ownSign, IScheduleDataManager scheduleDataManager,String strategy,IScheduleRuntimeManager sRuntimeManager) throws Exception {
		super(aFactory, baseTaskType, ownSign, scheduleDataManager);
		// ��ʼ��ScheduleServer
		this.currenScheduleServer = ScheduleServer.createScheduleServer(this.scheduleCenter.getSystemTime(), baseTaskType, "group", 1);
		this.currenScheduleServer.setManagerFactoryUUID(this.factory.getUuid());
		// ע�ᵱǰ�Ĵ���������
		scheduleCenter.registerScheduleServer(this.currenScheduleServer);
		this.runtimeManager = sRuntimeManager;
		// ��������
		this.assignScheduleTask();
		loadTask();
		this.taskTypeInfo.setPermitRunStartTime(strategy);
		// ����Ѿ�����1���TASK,OWN_SIGN����ϡ�����һ��û�лserver����Ϊ����
		this.scheduleCenter.clearExpireTaskTypeRunningInfo(baseTaskType, ScheduleUtil.getLocalIP() + "�������OWN_SIGN��Ϣ", this.taskTypeInfo.getExpireOwnSignInterval());
		// ��ȡִ�е�bean
		Object dealBean = factory.getBean(this.taskTypeInfo.getDealBeanName());
		if (dealBean == null) {
//			throw new Exception("SpringBean " + this.taskTypeInfo.getDealBeanName() + " ������");
			dealBean = Class.forName(this.taskTypeInfo.getDealBeanName());
		}
		if (dealBean instanceof IScheduleTaskDeal == false) {
			throw new Exception("SpringBean " + this.taskTypeInfo.getDealBeanName() + " û��ʵ�� IScheduleTaskDeal�ӿ�");
		}
		this.taskDealBean = (IScheduleTaskDeal) dealBean;
		// mBeanName = "pamirs:name=" + "schedule.ServerMananger." +
		// this.currenScheduleServer.getUuid();
		if (this.taskTypeInfo.getJudgeDeadInterval() < this.taskTypeInfo.getHeartBeatRate() * 5) {
			throw new Exception("�������ô������⣬������ʱ����������Ҫ���������̵߳�5������ǰ�������ݣ�JudgeDeadInterval = " + this.taskTypeInfo.getJudgeDeadInterval() + ",HeartBeatRate = " + this.taskTypeInfo.getHeartBeatRate());
		}


		this.heartBeatTimer = new Timer(this.currenScheduleServer.getTaskType() + "-" + this.currentSerialNumber + "-HeartBeat");
		// ע��timer����ʱ�������������ĸ��µ�ǰ��������������Ϣ
		this.heartBeatTimer.schedule(new HeartBeatTimerTask(this), new java.util.Date(System.currentTimeMillis() + 500), this.taskTypeInfo.getHeartBeatRate());

		initial();
	}
	
	
	private void loadTask() throws Exception {
		while (this.taskTypeInfo == null) {
			this.taskTypeInfo = scheduleCenter.loadGroupTaskTypeBaseInfo(baseTaskType, this.currenScheduleServer.getUuid());
			Thread.sleep(1000);
		}
	}
	public void initialRunningInfo() throws Exception{
		scheduleCenter.clearExpireScheduleServer(this.currenScheduleServer.getTaskType(),this.taskTypeInfo.getJudgeDeadInterval());
//		���������µ�server�б�
		List<String> list = scheduleCenter.loadScheduleServerNames(this.currenScheduleServer.getTaskType());
		if(scheduleCenter.isLeader(this.currenScheduleServer.getUuid(),list)){
	    	//�ǵ�һ����������������е���������
			log.debug(this.currenScheduleServer.getUuid() + ":" + list.size());
//			����taskItem��������ӽڵ�
//	    	this.scheduleCenter.initialRunningInfo4Static(this.currenScheduleServer.getBaseTaskType(), this.currenScheduleServer.getOwnSign(),this.currenScheduleServer.getUuid());
	    }
	 }
	public void initial() throws Exception{
		
    	new Thread(this.currenScheduleServer.getTaskType()  +"-" + this.currentSerialNumber +"-StartProcess"){
    		@SuppressWarnings("static-access")
			public void run(){
    			try{
    			   log.info("��ʼ��ȡ�����������...... of " + currenScheduleServer.getUuid());
					while (isRuntimeInfoInitial == false) {
						if (isStopSchedule == true) {
							log.debug("�ⲿ������ֹ����,�˳����ȶ��л�ȡ��" + currenScheduleServer.getUuid());
							return;
						}
						// log.error("isRuntimeInfoInitial = " + isRuntimeInfoInitial);
						try {
							initialRunningInfo();
//							�Ƿ��ʼ���ɹ�������taskItem��
							isRuntimeInfoInitial = scheduleCenter.isInitialRunningInfoSucuss(currenScheduleServer.getBaseTaskType(), currenScheduleServer.getOwnSign());
						} catch (Throwable e) {
							// ���Գ�ʼ�����쳣
							log.error(e.getMessage(), e);
						}
//						δ��ʼ���ɹ������߲������ȴ�
						if (isRuntimeInfoInitial == false) {
							Thread.currentThread().sleep(1000);
						}
					}
    			   int count =0;
    			   lastReloadTaskItemListTime = scheduleCenter.getSystemTime();
//    			   ��ǰ�ɴ�����taskС�ڣ��ȴ�
				   while(getCurrentScheduleTaskItemListNow().size() <= 0){
    				      if(isStopSchedule == true){
    				    	  log.debug("�ⲿ������ֹ����,�˳����ȶ��л�ȡ��" + currenScheduleServer.getUuid());
    				    	  return;
    				      }
    				      Thread.currentThread().sleep(1000);
        			      count = count + 1;
        			     // log.error("���Ի�ȡ���ȶ��У���" + count + "�� ") ;
    			   }
    			   String tmpStr ="";
    			   for(int i=0;i< currentTaskItemList.size();i++){
    				   if(i>0){
    					   tmpStr = tmpStr +",";    					   
    				   }
    				   tmpStr = tmpStr + currentTaskItemList.get(i);
    			   }
    			   log.info("��ȡ�����������У���ʼ���ȣ�" + tmpStr +"  of  "+ currenScheduleServer.getUuid());
    			   
    		    	//��������
    		    	taskItemCount = scheduleCenter.loadAllTaskItem(currenScheduleServer.getTaskType()).size();
    		    	//ֻ�����Ѿ���ȡ�����������к�ſ�ʼ������������    			   
    			   computerStart();
    			}catch(Exception e){
    				log.error(e.getMessage(),e);
    				String str = e.getMessage();
    				if(str.length() > 300){
    					str = str.substring(0,300);
    				}
    				startErrorInfo = "���������쳣��" + str;
    			}
    		}
    	}.start();
    }
	/**
	 * ��ʱ�������������ĸ��µ�ǰ��������������Ϣ��
	 * ������ֱ��θ��µ�ʱ������Ѿ������ˣ��������������������ڣ��������������������Ϣ��
	 * ��Ӧ�õ����µķ���������������ע�ᡣ
	 * @throws Exception 
	 */
	public void refreshScheduleServerInfo() throws Exception {
		try {
			rewriteScheduleInfo();
			// ���������Ϣû�г�ʼ���ɹ�������������صĴ���
			if (this.isRuntimeInfoInitial == false) {
				return;
			}

			// ���·�������
			this.assignScheduleTask();

			// �ж��Ƿ���Ҫ���¼���������У��������������̲���Ҫ�ļ��͵ȴ�
			boolean tmpBoolean = this.isNeedReLoadTaskItemList();
			if (tmpBoolean != this.isNeedReloadTaskItem) {
				// ֻҪ����ͬ����������Ҫ����װ�أ���Ϊ�������쳣��ʱ�������������е����飬�ָ�����Ҫ����װ�ء�
				synchronized (NeedReloadTaskItemLock) {
					this.isNeedReloadTaskItem = true;
				}
				rewriteScheduleInfo();
			}

			if (this.isPauseSchedule == true || this.processor != null && processor.isSleeping() == true) {
				// ��������Ѿ���ͣ�ˣ�����Ҫ���¶�ʱ���� cur_server �� req_server
				// �������û����ͣ��һ�����ܵ��õ�
				this.getCurrentScheduleTaskItemListNow();
			}
		} catch (Throwable e) {
			// ����ڴ������е��Ѿ�ȡ�õ����ݺ��������,���������߳�ʧ��ʱ���µ������ظ�
			this.clearMemoInfo();
			if (e instanceof Exception) {
				throw (Exception) e;
			} else {
				throw new Exception(e.getMessage(), e);
			}
		}
	}
	/**
	 * ��leader���·���������ÿ��server�ͷ�ԭ��ռ�е�������ʱ�������޸�����汾��
	 * @return
	 * @throws Exception
	 */
	public boolean isNeedReLoadTaskItemList() throws Exception{
		return this.lastFetchVersion < this.scheduleCenter.getReloadTaskItemFlag(this.currenScheduleServer.getTaskType());
	}
	/**
	 * ���ݵ�ǰ���ȷ���������Ϣ�����¼���������еĵ�������
	 * ����ķ�������Ҫ�������������ݷ������Ϊ�˱��������������ĸ������ã�ͨ���汾�����ﵽ����Ŀ��
	 * 
	 * 1����ȡ����״̬�İ汾��
	 * 2����ȡ���еķ�����ע����Ϣ�����������Ϣ
	 * 3������Ѿ������������ڵķ�����ע����Ϣ
	 * 3�����¼����������
	 * 4����������״̬�İ汾�š��ֹ�����
	 * 5����ϵ������еķ�����Ϣ
	 * @throws Exception 
	 */
	public void assignScheduleTask() throws Exception {
		scheduleCenter.clearExpireScheduleServer(this.currenScheduleServer.getTaskType(), 30000);
		List<String> serverList = scheduleCenter.loadScheduleServerNames(this.currenScheduleServer.getTaskType());

		if (scheduleCenter.isLeader(this.currenScheduleServer.getUuid(), serverList) == false) {
			if (log.isDebugEnabled()) {
				log.debug(this.currenScheduleServer.getUuid() + ":���Ǹ�����������Leader,ֱ�ӷ���");
			}
			return;
		}
		// ���ó�ʼ���ɹ���׼��������leaderת����ʱ���������߳����ʼ��ʧ��
		scheduleCenter.setInitialRunningInfoSucuss(this.currenScheduleServer.getBaseTaskType(), this.currenScheduleServer.getTaskType(), this.currenScheduleServer.getUuid());
//		�����ǰtask��item�µ�cur_server���ڵ�server��ֵ
		scheduleCenter.clearTaskItem(this.currenScheduleServer.getTaskType(), serverList);
//		���·���taskItem����
		scheduleCenter.assignTaskItem(this.currenScheduleServer.getTaskType(), this.currenScheduleServer.getUuid(), 1, serverList);
	}
	/**
	 * ���¼��ص�ǰ���������������
	 * 1���ͷŵ�ǰ���������У�������������������������������
	 * 2�����»�ȡ��ǰ�������Ĵ�������
	 * 
	 * Ϊ�˱���˲����Ĺ��ȣ��������������ݴ���������ϵͳ����һ������װ�ص�Ƶ�ʡ�����1����
	 * 
	 * �ر�ע�⣺
	 *   �˷����ĵ��ñ������ڵ�ǰ�������񶼴�����Ϻ���ܵ��ã������Ƿ�������к�������ݱ��ظ�����
	 */
	
	public List<TaskItemDefine> getCurrentScheduleTaskItemList() {
		try{
		if (this.isNeedReloadTaskItem == true) {			
			//�ر�ע�⣺��Ҫ�ж����ݶ����Ƿ��Ѿ����ˣ���������ڶ����л���ʱ���������ظ�����
			//��Ҫ�����̲߳����߾ͼ������ݵ�ʱ��һ����Ҫ����ж�
			if (this.processor != null) {
					while (this.processor.isDealFinishAllData() == false) {
						Thread.sleep(50);
					}
			}
			//������ʼ��������
			synchronized (NeedReloadTaskItemLock) {
				this.getCurrentScheduleTaskItemListNow();
				this.isNeedReloadTaskItem = false;
			}
		}
		this.lastReloadTaskItemListTime = this.scheduleCenter.getSystemTime();		
		return this.currentTaskItemList;		
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
	
	protected List<TaskItemDefine> getCurrentScheduleTaskItemListNow() throws Exception {
		// ��ȡ���µİ汾��
		this.lastFetchVersion = this.scheduleCenter.getReloadTaskItemFlag(this.currenScheduleServer.getTaskType());
		try {
			// �Ƿ�������Ķ���
			this.scheduleCenter.releaseDealTaskItem(this.currenScheduleServer.getTaskType(), this.currenScheduleServer.getUuid());
			// ���²�ѯ��ǰ�������ܹ������Ķ���
			// Ϊ�˱����������л��Ĺ����г��ֶ���˲��Ĳ�һ�£�������ڴ��еĶ���
			this.currentTaskItemList.clear();
			// ����װ�ص�ǰserver��Ҫ���������ݶ���
			this.currentTaskItemList = this.scheduleCenter.reloadDealTaskItem(this.currenScheduleServer.getTaskType(), this.currenScheduleServer.getUuid());

			// �������10���������ڻ�û�л�ȡ�����ȶ��У��򱨾�
			if (this.currentTaskItemList.size() == 0 && scheduleCenter.getSystemTime() - this.lastReloadTaskItemListTime > this.taskTypeInfo.getHeartBeatRate() * 10) {
				String message = "���ȷ�����" + this.currenScheduleServer.getUuid() + "[TASK_TYPE=" + this.currenScheduleServer.getTaskType() + "]����������������10���������ڣ��� û�л�ȡ��������������";
				log.warn(message);
			}

			if (this.currentTaskItemList.size() > 0) {
				// ����ʱ���
				this.lastReloadTaskItemListTime = scheduleCenter.getSystemTime();
			}

			return this.currentTaskItemList;
		} catch (Throwable e) {
			this.lastFetchVersion = -1; // ����ѰѰ汾������С�������������ʧ��
			if (e instanceof Exception) {
				throw (Exception) e;
			} else {
				throw new Exception(e);
			}
		}
	}
	public int getTaskItemCount(){
		 return this.taskItemCount;
	}

}
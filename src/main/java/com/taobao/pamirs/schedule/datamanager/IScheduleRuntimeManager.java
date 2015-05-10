package com.taobao.pamirs.schedule.datamanager;

import java.util.List;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.Watcher;

import com.taobao.pamirs.schedule.entity.manager.ScheduleTaskTypeRunningInfo;

/**
 * ��¼�����ִ����־��״̬�ӿ�
 *
 * @author : lihx
 * create date : 2014-11-28
 */
public interface IScheduleRuntimeManager {

	/**
	 * ���������ִ��״̬
	 * ��time�ڵ�����ʱû�м�¼ִ����Ϣ
	 * ��time�ڵ��ӽڵ�Ϊÿ�����񵱴ε�ִ�м�¼
	 * @param taskType ��������
	 * @param taskName item����
	 * @param time ʱ�����
	 */
	void createRunTimeSts(String taskType , String taskName , String time) throws Exception ;
	/**
	 * ����ִ�н���
	 * @param taskType ��������
	 * @param taskName item����
	 * @param time ʱ�����
	 */
	void finishRunTimeItem(String taskType , String taskName , String time) throws Exception;
	/**
	 * �������������Ŀ¼
	 * @param taskType ��������
	 * @param time
	 */
	void deleteFinishNode(String taskType , String time) throws Exception;
	/**
	 * ����ָ��ʱ�������ִ����Ϣ
	 * @param taskType ��������
	 * @param taskName item����
	 * @param time ʱ��
	 * @return
	 */
	ScheduleTaskTypeRunningInfo loadRunningInfo(String taskType , String taskName , String time) throws Exception;
	ScheduleTaskTypeRunningInfo loadRunningInfo(String path) throws Exception;
	/**
	 * 
	 * @param taskType ��������
	 * @param taskName item����
	 * @return
	 */
	List<ScheduleTaskTypeRunningInfo> loadAllRunningInfo(String taskType , String taskName) throws Exception;
	/**
	 * ������һ���������״̬
	 * @param taskType ��������
	 * @param taskName item����
	 * @param time ʱ��
	 * @param watcher ������
	 */
	boolean watchPreviousItem(String taskType, String previousTask, String time, CuratorWatcher watcher, int index) throws Exception;
	boolean watchPreviousItem(String taskType, String previousTask, String time, Watcher watcher, int index) throws Exception;
}

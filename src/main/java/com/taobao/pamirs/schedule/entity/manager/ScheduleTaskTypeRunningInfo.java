package com.taobao.pamirs.schedule.entity.manager;

import java.sql.Timestamp;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.pamirs.schedule.entity.manager.ScheduleTaskItem.TaskItemSts;


public class ScheduleTaskTypeRunningInfo {
	
	private long id;
    
	/**
     * �������ͣ�ԭʼ��������+"-"+ownSign
     */
	private String taskType;
	
	/**
	 * ԭʼ��������
	 */
	private String baseTaskType;
	
	/**
	 * ����
	 */
	private String ownSign;
	
	/**
	 * ���һ����������ʱ��
	 */
	private Timestamp lastAssignTime;
	
	/**
	 * ���һ��ִ���������ķ�����
	 */
	private String lastAssignUUID;
	
	private Timestamp gmtCreate;
	
	private Timestamp gmtModified;
	
	private TaskItemSts sts = TaskItemSts.ACTIVTE;
	
	public String toString(){
		return ToStringBuilder.reflectionToString(this);
	}
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getOwnSign() {
		return ownSign;
	}
	public void setOwnSign(String ownSign) {
		this.ownSign = ownSign;
	}
	public String getTaskType() {
		return taskType;
	}
	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}
	public Timestamp getLastAssignTime() {
		return lastAssignTime;
	}
	public void setLastAssignTime(Timestamp lastAssignTime) {
		this.lastAssignTime = lastAssignTime;
	}
	public String getLastAssignUUID() {
		return lastAssignUUID;
	}
	public void setLastAssignUUID(String lastAssignUUID) {
		this.lastAssignUUID = lastAssignUUID;
	}
	public Timestamp getGmtCreate() {
		return gmtCreate;
	}
	public void setGmtCreate(Timestamp gmtCreate) {
		this.gmtCreate = gmtCreate;
	}
	public Timestamp getGmtModified() {
		return gmtModified;
	}
	public void setGmtModified(Timestamp gmtModified) {
		this.gmtModified = gmtModified;
	}

	public String getBaseTaskType() {
		return baseTaskType;
	}

	public void setBaseTaskType(String baseTaskType) {
		this.baseTaskType = baseTaskType;
	}

	public TaskItemSts getSts() {
		return sts;
	}

	public void setSts(TaskItemSts sts) {
		this.sts = sts;
	}
	
}

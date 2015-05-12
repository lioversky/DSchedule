package com.chanct.schedule.entity.strategy;

import org.apache.commons.lang.builder.ToStringBuilder;

public class ScheduleStrategy {
	public enum Kind{Schedule,Java,Bean,Group}
	public enum Type{StartRunEveryTime,RunByNums,CronExp}
	/**
	 * 任务类型
	 */
	private String strategyName;

	private String[] IPList;
	/**
	 * 单JVM最大线程组数量
	 */
	private int numOfSingleServer;
	/**
	 * 指定需要执行调度的机器数量
	 */
	private int assignNum;
	
	private String ownSign = "BASE";
	private Kind kind; 
	
	/**
	 * Schedule Name,Class Name、Bean Name
	 */
	private String taskName; 
	
    /**
     * 服务状态: pause,resume
     */
    private String sts = STS_RESUME;
	
    public static String STS_PAUSE="pause";
    public static String STS_RESUME="resume";
    
    private Type executeType = Type.CronExp;
    /**
     * 执行策略，当type为StartRunEveryTime时无效
     * type为RunByNums时，此值为执行次数
     * type为CronExp时，此值为表达式
     */
    private String executeStrategy;
    /**
     * 允许执行的开始时间
     */
    private String permitRunStartTime;
    /**
     * 允许执行的开始时间
     */
    private String permitRunEndTime;
	public String getPermitRunStartTime() {
		return permitRunStartTime;
	}

	public void setPermitRunStartTime(String permitRunStartTime) {
		this.permitRunStartTime = permitRunStartTime;
		if(this.permitRunStartTime != null && this.permitRunStartTime.trim().length() ==0){
			this.permitRunStartTime = null;
		}	
	}
	public String getPermitRunEndTime() {
		return permitRunEndTime;
	}

	public void setPermitRunEndTime(String permitRunEndTime) {
		this.permitRunEndTime = permitRunEndTime;
		if(this.permitRunEndTime != null && this.permitRunEndTime.trim().length() ==0){
			this.permitRunEndTime = null;
		}	

	}
	
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
	
	public Type getExecuteType() {
		return executeType;
	}

	public void setExecuteType(Type executeType) {
		this.executeType = executeType;
	}

	public String getExecuteStrategy() {
		return executeStrategy;
	}

	public void setExecuteStrategy(String executeStrategy) {
		this.executeStrategy = executeStrategy;
	}

	public String getOwnSign() {
		return ownSign;
	}

	public void setOwnSign(String ownSign) {
		this.ownSign = ownSign;
	}

	public String getStrategyName() {
		return strategyName;
	}

	public void setStrategyName(String strategyName) {
		this.strategyName = strategyName;
	}

	public int getAssignNum() {
		return assignNum;
	}

	public void setAssignNum(int assignNum) {
		this.assignNum = assignNum;
	}

	public String[] getIPList() {
		return IPList;
	}

	public void setIPList(String[] iPList) {
		IPList = iPList;
	}

	public void setNumOfSingleServer(int numOfSingleServer) {
		this.numOfSingleServer = numOfSingleServer;
	}

	public int getNumOfSingleServer() {
		return numOfSingleServer;
	}
	public Kind getKind() {
		return kind;
	}
	public void setKind(Kind kind) {
		this.kind = kind;
	}
	public String getTaskName() {
		return taskName;
	}
	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}


	public String getSts() {
		return sts;
	}

	public void setSts(String sts) {
		this.sts = sts;
	}	
}

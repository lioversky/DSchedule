package com.chanct.schedule.entity.strategy;

public class ManagerFactoryInfo {
	private String uuid;
	private boolean start;
	private String ip;
	private String hostName;
//	当前执行策略数
	private int runs = 0;
//	分本策略数，用于确定此节点是否需要重新加载任务
	private int assign = 0;
	
	
	public int getRuns() {
		return runs;
	}

	public void setRuns(int runs) {
		this.runs = runs;
	}

	public int getAssign() {
		return assign;
	}

	public void setAssign(int assign) {
		this.assign = assign;
	}

	public void setStart(boolean start) {
		this.start = start;
	}

	public boolean isStart() {
		return start;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getUuid() {
		return uuid;
	}
	

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
}

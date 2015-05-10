package com.taobao.pamirs.schedule.bean;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.bean.IScheduleTaskDealSingle;
import com.taobao.pamirs.schedule.entity.TaskItemDefine;

/**
 * 
 * 
 * @author : lihx create date : 2014-12-4
 */
public class LocalCommandBean implements IScheduleTaskDealSingle<String> {
	protected static transient Logger logger = LoggerFactory.getLogger(LocalCommandBean.class);
	@Override
	public List<String> selectTasks(String taskParameter, String ownSign, int taskItemNum, List<TaskItemDefine> taskItemList, int eachFetchDataNum) throws Exception {
		return null;
	}

	@Override
	public Comparator<String> getComparator() {
		return null;
	}

	@Override
	public boolean execute(String task, String ownSign) throws Exception {
		logger.info("start to run process, command is :"+task);
		
		boolean result = process(task);
		return result;
	}
	private boolean process(String cmd) {
		try {
			Process p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
			p.destroy();
			p = null;
			logger.info("job end!!!!!");
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
	}

	
}

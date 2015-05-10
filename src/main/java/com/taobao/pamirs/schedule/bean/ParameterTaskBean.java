package com.taobao.pamirs.schedule.bean;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.entity.TaskItemDefine;

/**
 * 单个任务处理实现
 * 
 * @author xuannan
 * 
 */
public class ParameterTaskBean implements IScheduleTaskDealSingle<String> {
	protected static transient Logger log = LoggerFactory.getLogger(ParameterTaskBean.class);

	public Comparator<String> getComparator() {
		return new Comparator<String>() {
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}

			public boolean equals(Object obj) {
				return this == obj;
			}
		};
	}

	public List<String> selectTasks(String taskParameter,String ownSign, int taskItemNum,
			List<TaskItemDefine> queryCondition, int fetchNum) throws Exception {
		List<String> result = new ArrayList<String>();
		return result;
	}

	public boolean execute(String task, String ownSign) throws Exception {
		Thread.sleep(500);
		log.info("处理任务["+ownSign+"]:" + task);
		return true;
	}
}
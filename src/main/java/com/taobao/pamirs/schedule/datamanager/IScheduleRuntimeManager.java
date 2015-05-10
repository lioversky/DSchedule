package com.taobao.pamirs.schedule.datamanager;

import java.util.List;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.Watcher;

import com.taobao.pamirs.schedule.entity.manager.ScheduleTaskTypeRunningInfo;

/**
 * 记录任务的执行日志、状态接口
 *
 * @author : lihx
 * create date : 2014-11-28
 */
public interface IScheduleRuntimeManager {

	/**
	 * 创建任务的执行状态
	 * 在time节点里暂时没有记录执行信息
	 * 在time节点子节点为每个任务当次的执行记录
	 * @param taskType 任务名称
	 * @param taskName item名称
	 * @param time 时间参数
	 */
	void createRunTimeSts(String taskType , String taskName , String time) throws Exception ;
	/**
	 * 任务执行结束
	 * @param taskType 任务名称
	 * @param taskName item名称
	 * @param time 时间参数
	 */
	void finishRunTimeItem(String taskType , String taskName , String time) throws Exception;
	/**
	 * 任务结束后清理目录
	 * @param taskType 任务名称
	 * @param time
	 */
	void deleteFinishNode(String taskType , String time) throws Exception;
	/**
	 * 加载指定时间参数的执行信息
	 * @param taskType 任务名称
	 * @param taskName item名称
	 * @param time 时间
	 * @return
	 */
	ScheduleTaskTypeRunningInfo loadRunningInfo(String taskType , String taskName , String time) throws Exception;
	ScheduleTaskTypeRunningInfo loadRunningInfo(String path) throws Exception;
	/**
	 * 
	 * @param taskType 任务名称
	 * @param taskName item名称
	 * @return
	 */
	List<ScheduleTaskTypeRunningInfo> loadAllRunningInfo(String taskType , String taskName) throws Exception;
	/**
	 * 监听上一个任务完成状态
	 * @param taskType 任务名称
	 * @param taskName item名称
	 * @param time 时间
	 * @param watcher 监听器
	 */
	boolean watchPreviousItem(String taskType, String previousTask, String time, CuratorWatcher watcher, int index) throws Exception;
	boolean watchPreviousItem(String taskType, String previousTask, String time, Watcher watcher, int index) throws Exception;
}

package com.chanct.schedule.bean;

import java.util.Comparator;
import java.util.List;



/**
 * 调度器对外的基础接口
 * @author xuannan
 *
 * @param <T> 任务类型
 */
public interface IScheduleTaskDeal<T> {

/**
 * 根据条件，查询当前调度服务器可处理的任务	
 * @param taskParameter 任务的自定义参数
 * @param ownSign 当前环境名称 
 * @param taskItemNum 当前任务类型的任务队列数量
 * @param taskItemList 当前调度服务器，分配到的可处理队列
 * @param eachFetchDataNum 每次获取数据的数量
 * @return
 * @throws Exception
 */
public List<T> selectTasks(String taskParameter,String ownSign,int taskItemNum,List<T> taskItemList,int eachFetchDataNum) throws Exception;

/**
 * 获取任务的比较器,主要在NotSleep模式下需要用到
 * @return
 */
public Comparator<T> getComparator();

}

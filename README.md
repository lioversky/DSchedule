# DSchedule
淘宝tbschedule的改进版，增加单独进程启动不放在容器中可独立部署；
增加任务的分配执行策略，根据当前执行节点平均分配；
增加简单分组执行；

引入curator管理zookeeper

主要功能
集群的任务分配
	运行任务少的节点优先获得新任务
	任务可以在指定ip或ip段运行执行
	执行节点挂掉时任务分配到其它节点
任务执行策略
	按启动类型划分：开始即执行、简单次数执行、复杂策略执行
	按执行类型划分：固定ip或ip段执行、单实例多实例执行
	按组顺序执行
集群中的任务信息
	任务任务执行信息
	任务的执行历史
执行节点信息
	每个节点的状态、所执行任务
集群启动、挂掉、重启和扩展
	新增节点或重新启动时任务分摊
	挂掉时其它可执行任务的节点接管任务
	挂掉节点自动唤醒
任务下发
	Webservice
	Api
	页面
异常处理机制
	本机重试
	其它节点重试
状态监测
	Jmx
外部插件
	执行历史输出
	任务存储输出


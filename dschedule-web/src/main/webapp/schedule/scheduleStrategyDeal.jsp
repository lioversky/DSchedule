<%@page import="com.chanct.schedule.ConsoleManager"%>
<%@page
	import="com.chanct.schedule.entity.strategy.ScheduleStrategy"%>
<%@ page contentType="text/html; charset=utf-8"%>
<html>
<head>
<title>创建调度任务</title>
</head>
<body bgcolor="#ffffff">

	<%
		String action = (String) request.getParameter("action");
			String result = "";
			boolean isRefreshParent = false;
			ScheduleStrategy scheduleStrategy = new ScheduleStrategy();
			scheduleStrategy.setStrategyName(request.getParameter("strategyName"));
			try {
		if (action.equalsIgnoreCase("createScheduleStrategy") || action.equalsIgnoreCase("editScheduleStrategy")) {
				scheduleStrategy.setKind(ScheduleStrategy.Kind.valueOf(request.getParameter("kind")));
				scheduleStrategy.setTaskName(request.getParameter("taskName"));
				scheduleStrategy.setOwnSign(request.getParameter("ownSign"));
				scheduleStrategy.setPermitRunEndTime(request.getParameter("permitRunEndTime"));
				scheduleStrategy.setPermitRunStartTime(request.getParameter("permitRunStartTime"));
				scheduleStrategy.setExecuteStrategy(request.getParameter("executeStrategy"));
				scheduleStrategy.setExecuteType(ScheduleStrategy.Type.valueOf(request.getParameter("executeType")));
				
				scheduleStrategy.setNumOfSingleServer(request.getParameter("numOfSingleServer") == null ? 0 : Integer.parseInt(request.getParameter("numOfSingleServer")));
				scheduleStrategy.setAssignNum(request.getParameter("assignNum") == null ? 0 : Integer.parseInt(request.getParameter("assignNum")));
				String ips = request.getParameter("ips");
				if ( ips== null||"".equals(ips)) {
//					scheduleStrategy.setIPList(new String[0]);
				} else {
					scheduleStrategy.setIPList(request.getParameter("ips").split(","));
				}
				if (action.equalsIgnoreCase("createScheduleStrategy")) {
					ConsoleManager.getScheduleStrategyManager().createScheduleStrategy(scheduleStrategy);
					isRefreshParent = true;
				} else if (action.equalsIgnoreCase("editScheduleStrategy")) {
					ConsoleManager.getScheduleStrategyManager().updateScheduleStrategy(scheduleStrategy);
					isRefreshParent = true;
				}
			} else if (action.equalsIgnoreCase("deleteScheduleStrategy")) {
				ConsoleManager.getScheduleStrategyManager().deleteMachineStrategy(scheduleStrategy.getStrategyName());
				isRefreshParent = true;
			} else if (action.equalsIgnoreCase("pauseTaskType")) {
				ConsoleManager.getScheduleStrategyManager().pause(scheduleStrategy.getStrategyName());
				isRefreshParent = true;
			} else if (action.equalsIgnoreCase("resumeTaskType")) {
				ConsoleManager.getScheduleStrategyManager().resume(scheduleStrategy.getStrategyName());
				isRefreshParent = true;
			} else {
				throw new Exception("不支持的操作：" + action);
			}
		} catch (Throwable e) {
			e.printStackTrace();
			result = "ERROR:" + e.getMessage();
			isRefreshParent = false;
		}
	%>
	<%=result%>
</body>
</html>
<%
	if (isRefreshParent == true) {
%>
<script>
	parent.location.reload();
</script>
<%
	}
%>
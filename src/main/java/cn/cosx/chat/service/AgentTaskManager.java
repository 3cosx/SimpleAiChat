package cn.cosx.chat.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class AgentTaskManager {

    private final Map<String,TaskInfo> taskInfoMap = new ConcurrentHashMap<>();

    public boolean hasTask(String conversationId) {
        return taskInfoMap.containsKey(conversationId);
    }

    public void setDisposable(String currentConversationId, Disposable disposable) {
        TaskInfo taskInfo = taskInfoMap.get(currentConversationId);
        if(taskInfo != null){
            taskInfo.setDisposable(disposable);
        }
    }

    public void stopTask(String conversationId) {
        TaskInfo removed = taskInfoMap.remove(conversationId);
        if (removed != null) {
            log.info("任务已移除: conversationId={}", conversationId);
        }
    }

    /**
     * 任务信息
     */
    public static class TaskInfo {
        private final Sinks.Many<String> sink;
        private Disposable disposable;

        public TaskInfo(Sinks.Many<String> sink) {
            this.sink = sink;
        }

        public Sinks.Many<String> getSink() {
            return sink;
        }

        public Disposable getDisposable() {
            return disposable;
        }

        public void setDisposable(Disposable disposable) {
            this.disposable = disposable;
        }
    }

    public TaskInfo registerTask(String conversationId,Sinks.Many<String> sinks){
        TaskInfo taskInfo = taskInfoMap.get(conversationId);
        if(Objects.nonNull(taskInfo)){
            log.warn("当前会话正在执行");
            return null;
        }
        taskInfo = new TaskInfo(sinks);
        taskInfoMap.put(conversationId,taskInfo);
        log.info("任务已注册: conversationId={}, runningTasks={}", conversationId, taskInfoMap.size());
        return taskInfo;
    }
}

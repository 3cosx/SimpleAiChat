package cn.cosx.chat.agent.web;

import cn.cosx.chat.adapter.SessionAdapter;
import cn.cosx.chat.agent.BaseAgent;
import cn.cosx.chat.common.response.AgentResponse;
import cn.cosx.chat.context.ContextCompactor;
import cn.cosx.chat.context.ContextPolicy;
import cn.cosx.chat.entity.dto.AgentRoundSearchResult;
import cn.cosx.chat.entity.dto.RoundMode;
import cn.cosx.chat.entity.dto.RoundState;
import cn.cosx.chat.entity.record.ChatRequest;
import cn.cosx.chat.entity.record.ChunkSegment;
import cn.cosx.chat.entity.record.SearchResult;
import cn.cosx.chat.prompt.ReactAgentPrompts;
import cn.cosx.chat.service.AgentTaskManager;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import io.micrometer.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.AdvisorParams;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.api.Advisor;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.messages.*;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.deepseek.DeepSeekAssistantMessage;
import org.springframework.ai.model.tool.ToolCallingChatOptions;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.util.CollectionUtils;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class WebSearchReactAgent extends BaseAgent {

    private ChatClient chatClient;
    private final String systemPrompt;
    private final List<ToolCallback> tools;
    private final int maxRounds;
    private final List<Advisor> advisors;
    private final ContextCompactor contextCompactor;

    private WebSearchReactAgent(String name,
                                String agentType,
                                ChatModel chatModel,
                                ChatMemory chatMemory,
                                String systemPrompt,
                                List<ToolCallback> tools,
                                int maxRounds,
                                AgentTaskManager taskManager,
                                SessionAdapter sessionAdapter,
                                List<Advisor> advisors,
                                ContextPolicy contextPolicy) {
        super(name, agentType, chatModel);
        this.chatMemory = chatMemory;
        this.systemPrompt = systemPrompt;
        this.tools = tools;
        this.sessionAdapter = sessionAdapter;
        this.taskManager = taskManager;
        this.maxRounds = maxRounds;
        this.advisors = advisors;
        this.contextCompactor = new ContextCompactor(contextPolicy == null ? ContextPolicy.defaults() : contextPolicy, chatModel);
        initClient();
    }

    @Override
    public Flux<String> execute(ChatRequest request) {
        return stream(request.conversationId(), request.question());
    }


    public Flux<String> stream(String conversationId, String query) {
        List<Message> messages = Collections.synchronizedList(new ArrayList<>());
        log.info("Agent开始执行: agent={}, conversationId={}, query={}", name, conversationId, abbreviate(query, 200));

        Flux<String> runningTask = checkTaskRunning(conversationId);
        if(runningTask!= null){
            log.warn("会话已有任务执行中: conversationId={}", conversationId);
            return runningTask;
        }

        Sinks.Many<String> sinks = Sinks.many().unicast().onBackpressureBuffer();
        AgentTaskManager.TaskInfo taskInfo = registerTask(conversationId, sinks);
        if (taskInfo == null && conversationId != null && taskManager != null) {
            log.warn("任务注册失败，会话正在执行: conversationId={}", conversationId);
            return Flux.error(new IllegalStateException("该会话正在执行中，请稍后再试"));
        }
        log.info("任务注册成功: conversationId={}, maxRounds={}, availableTools={}", conversationId, maxRounds,
                tools == null ? 0 : tools.size());

        messages.add(new SystemMessage(ReactAgentPrompts.getWebSearchPrompt()));
        if(StringUtils.isNotBlank(systemPrompt)){
            messages.add(new SystemMessage(systemPrompt));
        }

        loadHistoryMessages(conversationId,messages,true,true);

        messages.add(new UserMessage("<question>" + query + "</question>"));

        currentConversationId = conversationId;
        currentQuestion = query;
        //保存信息
        sessionAdapter.saveUserMessage(conversationId,query);
        log.info("用户消息已保存: conversationId={}, queryLength={}", conversationId, query == null ? 0 : query.length());

        AtomicInteger round = new AtomicInteger(0);
        round.set(0);

        AtomicBoolean hasSentFinalRes = new AtomicBoolean(false);
        hasSentFinalRes.set(false);

        AgentRoundSearchResult agentRound = new AgentRoundSearchResult();

        scheduleRound(agentRound,round,hasSentFinalRes,messages,sinks);

        StringBuilder finalAnswer = new StringBuilder();

        StringBuilder thinking = new StringBuilder();


        return sinks.asFlux()
                .doOnNext(chunk->{
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(chunk);
                        String type = jsonObject.getString("type");
                        String content = jsonObject.getString("content");

                        if(AgentResponse.TYPE_TEXT.equals(type)){
                            finalAnswer.append(content);
                        }else if(AgentResponse.TYPE_THINKING.equals(type)){
                            thinking.append(content);
                        }
                    } catch (Exception e) {
                        finalAnswer.append(chunk);
                    }
                })
                .doOnCancel(() ->{
                    log.warn("客户端取消流式响应: conversationId={}", conversationId);
                    hasSentFinalRes.set(true);
                    if (taskManager != null) {
                        taskManager.stopTask(conversationId);
                    }
                })
                .doFinally(signalType ->{
                    log.info("Agent流结束: conversationId={}, signal={}, finalAnswerLength={}, thinkingLength={}, usedTools={}, references={}",
                            conversationId, signalType, finalAnswer.length(), thinking.length(), usedTools,
                            agentRound.searchResults.size());
                    sessionAdapter.saveAssisantMessage(conversationId,finalAnswer.toString(),thinking.toString(),usedTools,currentRecommendations,agentRound.searchResults);
                    // 流结束时移除任务
                    if (taskManager != null) {
                        taskManager.stopTask(conversationId);
                    }

                });
    }

    private void scheduleRound(AgentRoundSearchResult agentRound,AtomicInteger round, AtomicBoolean hasSentFinalRes,List<Message> messages,Sinks.Many<String> sinks) {
        int currentRound = round.incrementAndGet();
        RoundState state = new RoundState();

        logRoundSeparator("AI轮次开始", currentRound);
        log.info("AI轮次开始: conversationId={}, round={}, messages={}, tools={}",
                currentConversationId, currentRound, messages.size(), tools == null ? 0 : tools.size());
        compactMessagesBeforeRequest(messages);
        List<Message> requestMessages = sanitizeMessages(messages);

        Disposable disposable = chatClient.prompt()
                .advisors(AdvisorParams.toolCallingAdvisorAutoRegister(false))
                .messages(requestMessages)
                .stream()
                .chatResponse()
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(chunk -> processChunk(chunk, state, sinks))
                .doOnComplete(() -> {
                    log.info("AI轮次完成: conversationId={}, round={}, mode={}, textLength={}, toolCalls={}",
                            currentConversationId, currentRound, state.mode, state.textBuffer.length(), state.toolCalls.size());
                    finishRound(messages, state, round, agentRound, hasSentFinalRes, sinks);
                })
                .subscribe(
                        ignored -> {
                        },
                        err -> handleRoundError("AI轮次异常", currentRound, hasSentFinalRes, sinks, err)
                );

        if(currentConversationId != null && taskManager != null){
            taskManager.setDisposable(currentConversationId,disposable);
        }
    }

    private void finishRound(List<Message> messages, RoundState state, AtomicInteger round, AgentRoundSearchResult agentRound,
                             AtomicBoolean hasSentFinalRes, Sinks.Many<String> sinks) {

        if(state.mode != RoundMode.TOOL_CALL){
            String referenced = "";
            String finalAnswer = state.textBuffer.toString();
            logRoundSeparator("最终回答", round.get());

            if(!agentRound.searchResults.isEmpty()){
                String reference = JSON.toJSONString(agentRound.searchResults);
                referenced = createReferenceResponse(reference);
                sinks.tryEmitNext(referenced);
            }

            if(enableRecommendations){
                String recommendations = generateRecommendations(finalAnswer);
                if (recommendations != null) {
                    currentRecommendations = recommendations; // 保存用于数据库存储
                    String recommendJson = createRecommendResponse(recommendations);
                    sinks.tryEmitNext(recommendJson);
                    log.info("推荐问题已发送: conversationId={}, recommendations={}", currentConversationId,
                            abbreviate(recommendations, 300));
                }
            }

            hasSentFinalRes.set(true);
            sinks.tryEmitComplete();
            log.info("最终回答完成: conversationId={}, round={}", currentConversationId, round.get());
            return;
        }

        AssistantMessage assistantMsg = AssistantMessage.builder()
                .content("")
                .toolCalls(state.toolCalls)
                .build();
        messages.add(assistantMsg);
        log.info("AI请求工具调用: conversationId={}, round={}, toolCalls={}",
                currentConversationId, round.get(), describeToolCalls(state.toolCalls));

        if(round.get() >= maxRounds){
            log.warn("达到最大轮次，强制总结: conversationId={}, round={}, maxRounds={}",
                    currentConversationId, round.get(), maxRounds);
            forceSummary(agentRound,  round, hasSentFinalRes, messages,sinks);
            return ;
        }

        executeToolCalls(sinks, state.toolCalls, messages, hasSentFinalRes, state, agentRound, () -> {
            if (!hasSentFinalRes.get()) {
                scheduleRound(agentRound,round,hasSentFinalRes,messages,sinks);
            }
        });

    }

    private void executeToolCalls(Sinks.Many<String> sinks, List<AssistantMessage.ToolCall> toolCalls, List<Message> messages,
                                  AtomicBoolean hasSentFinalRes, RoundState state, AgentRoundSearchResult agentRound, Runnable onComplete) {

        AtomicInteger completedCount = new AtomicInteger(0);
        int total = state.toolCalls.size();

        Map<String, ToolResponseMessage.ToolResponse> responseMap = new ConcurrentHashMap<>();
        log.info("开始执行工具批次: conversationId={}, total={}, toolCalls={}",
                currentConversationId, total, describeToolCalls(toolCalls));

        for (AssistantMessage.ToolCall tc : toolCalls) {
            Schedulers.boundedElastic().schedule(()->{

                if(hasSentFinalRes.get()){
                    completeToolUse(completedCount,total,responseMap,toolCalls,messages,onComplete);
                    return;
                }

                String toolName = tc.name();
                String arguments = tc.arguments();
                long startedAt = System.currentTimeMillis();
                log.info("工具调用开始: conversationId={}, toolCallId={}, toolName={}, arguments={}",
                        currentConversationId, tc.id(), toolName, abbreviate(arguments, 800));

                ToolCallback toolCallback = findTool(toolName);

                if(toolCallback ==null){
                    log.warn("工具未找到: conversationId={}, toolCallId={}, toolName={}",
                            currentConversationId, tc.id(), toolName);
                    responseMap.put(tc.id(), new ToolResponseMessage.ToolResponse(
                            tc.id(), toolName, "{ \"error\": \"工具未找到：" + toolName + "\" }"));
                    completeToolUse(completedCount,total,responseMap,toolCalls,messages,onComplete);
                    return ;
                }

                if(toolName.contains("search")){
                    JSONObject args = JSON.parseObject(arguments);
                    String query = (String) args.get("query");
                    // 发送 thinking 消息，表示正在搜索相关信息
                    String queryThink = StringUtils.isNotBlank(query) ? "🔍 正在搜索信息: " + query + "\n" : "🔍 正在搜索相关信息\n";
                    sinks.tryEmitNext(createThinkingResponse(queryThink));
                }

                try {
                    String result = toolCallback.call(arguments);

                    if(toolName.contains("tavily")){
                        parseTavilyResult(result,agentRound);
                    }
                    responseMap.put(tc.id(), new ToolResponseMessage.ToolResponse(tc.id(),tc.name(),result));


                }catch (Exception e){
                    log.error("工具调用异常: conversationId={}, toolCallId={}, toolName={}, costMs={}",
                            currentConversationId, tc.id(), toolName, System.currentTimeMillis() - startedAt, e);
                    responseMap.put(tc.id(),new ToolResponseMessage.ToolResponse(tc.id(),tc.name(),"{\"error\": \"发生异常：\""+ e +"\"}"));
                }finally {
                    usedTools.add(toolName);
                    completeToolUse(completedCount,total,responseMap,toolCalls,messages,onComplete);
                }
            });
        }

    }



    private void completeToolUse(AtomicInteger completedCount, int total, Map<String, ToolResponseMessage.ToolResponse> responseMap,
                                 List<AssistantMessage.ToolCall> toolCalls, List<Message> messages, Runnable onComplete) {
        int current = completedCount.incrementAndGet();

        if(current >= total){
            List<ToolResponseMessage.ToolResponse> responses = new ArrayList<>();
            for (AssistantMessage.ToolCall toolCall : toolCalls) {
                ToolResponseMessage.ToolResponse toolResponse = responseMap.get(toolCall.id());
                if(toolResponse == null){
                    responses.add(new ToolResponseMessage.ToolResponse(toolCall.id(),toolCall.name(),"{ \"error\": \"工具响应丢失\" }"));
                }else{
                    responses.add(new ToolResponseMessage.ToolResponse(toolCall.id(),toolCall.name(),toolResponse.responseData()));
                }
            }

            //添加工具调用结果到上下文
            messages.add(ToolResponseMessage.builder()
                    .responses(sanitizeToolResponses(responses))
                    .build());
            logRoundSeparator("工具批次完成，准备进入下一轮", null);
            log.info("工具批次完成并写入上下文: conversationId={}, completed={}, responseCount={}, messages={}",
                    currentConversationId, current, responses.size(), messages.size());
            onComplete.run();
        }

    }

    private ToolCallback findTool(String toolName) {
        return  tools.stream().filter(tc->tc.getToolDefinition().name().equals(toolName)).findFirst().orElse(null);
    }


    private void processChunk(ChatResponse chunk, RoundState state, Sinks.Many<String> sinks) {

        if (chunk == null || chunk.getResult() == null ||
                chunk.getResult().getOutput() == null) {
            return;
        }

        Generation generation = chunk.getResult();
        List<AssistantMessage.ToolCall> toolCalls = generation.getOutput().getToolCalls();

        if(!CollectionUtils.isEmpty(toolCalls)){
            state.mode = RoundMode.TOOL_CALL;

            for (AssistantMessage.ToolCall toolCall : toolCalls) {
                mergeTools(toolCall,state);
            }
            log.info("收到工具调用chunk: conversationId={}, toolCalls={}",
                    currentConversationId, describeToolCalls(toolCalls));
            return;
        }

        //todo 解析
        String text = generation.getOutput().getText();
        if(Objects.nonNull(text)){
            List<ChunkSegment> chunkSegments = parseChunk(chunk,sinks);
            state.textBuffer.append(text);
            if (log.isDebugEnabled()) {
                log.debug("收到AI文本chunk: conversationId={}, textLength={}, segments={}",
                        currentConversationId, text.length(), chunkSegments.size());
            }
        }

    }

    private void mergeTools(AssistantMessage.ToolCall incoming,RoundState state) {
        String id = incoming.id();
        for (int i = 0; i < state.toolCalls.size(); i++) {
            AssistantMessage.ToolCall existing = state.toolCalls.get(i);
            if(existing.id().equals(id)){
                String mergedArgs = Objects.toString(existing.arguments(), "") + Objects.toString(incoming.arguments(), "");
                state.getToolCalls().set(i,new AssistantMessage.ToolCall(existing.id(),"function",existing.name(),mergedArgs));
                log.debug("合并工具调用chunk: conversationId={}, toolCallId={}, toolName={}, argumentsLength={}",
                        currentConversationId, existing.id(), existing.name(), mergedArgs.length());
                return ;
            }
        }
        state.toolCalls.add(incoming);
        log.debug("新增工具调用chunk: conversationId={}, toolCallId={}, toolName={}",
                currentConversationId, incoming.id(), incoming.name());
    }

    private void forceSummary(AgentRoundSearchResult agentRound, AtomicInteger round,
                              AtomicBoolean hasSentFinalRes, List<Message> messages,Sinks.Many<String> sinks) {

        if (hasSentFinalRes.get()){
            return ;
        }
        logRoundSeparator("强制总结", round.get());
        log.info("强制总结开始: conversationId={}, round={}, references={}",
                currentConversationId, round.get(), agentRound.searchResults.size());

        List<Message> forceMessages = new ArrayList<>();
        forceMessages.add(new SystemMessage(ReactAgentPrompts.getWebSearchPrompt()));
        if(StringUtils.isNotBlank(systemPrompt)){
            forceMessages.add(new SystemMessage(systemPrompt));
        }

        for (Message message : messages) {
            if (! (message instanceof SystemMessage)) {
                forceMessages.add(message);
            }
        }

        forceMessages.add(new UserMessage("""
                你已达到最大推理轮次限制。
                请基于当前已有的上下文信息，
                直接给出最终答案。
                禁止再调用任何工具。
                如果信息不完整，请合理总结和说明。
                """));
        messages.clear();
        messages.addAll(forceMessages);

        StringBuilder finalAnswer = new StringBuilder();
        compactMessagesBeforeRequest(messages);
        List<Message> requestMessages = sanitizeMessages(messages);

        Disposable disposable = chatClient.prompt()
                .messages(requestMessages)
                .stream()
                .chatResponse()
                .doOnNext(chunk -> {
                    if (chunk == null || chunk.getResult() == null || chunk.getResult().getOutput() == null) {
                        return;
                    }

                    List<ChunkSegment> chunkSegments = parseChunk(chunk,sinks);
                    if (!CollectionUtils.isEmpty(chunkSegments)) {
                        for (ChunkSegment chunkSegment : chunkSegments) {
                            if (chunkSegment.isThinking()) {
                                sinks.tryEmitNext(createThinkingResponse(chunkSegment.content()));
                            } else {
                                sinks.tryEmitNext(createTextResponse(chunkSegment.content()));
                                finalAnswer.append(chunkSegment.content());
                            }
                        }
                    }


                })
                .doOnComplete(() -> {
                    String referenceJson = "";
                    String answer = finalAnswer.toString();
                    log.info("强制总结AI调用完成: conversationId={}, answerLength={}", currentConversationId, answer.length());
                    if (!agentRound.searchResults.isEmpty()) {
                        String reference = JSON.toJSONString(agentRound.searchResults);
                        referenceJson = createReferenceResponse(reference);
                        sinks.tryEmitNext(referenceJson);
                    }

                    if (enableRecommendations) {
                        String recommendations = generateRecommendations(answer);
                        sinks.tryEmitNext(createRecommendResponse(recommendations));
                        log.info("强制总结推荐问题已发送: conversationId={}, recommendations={}",
                                currentConversationId, abbreviate(recommendations, 300));
                    }


                    hasSentFinalRes.set(true);
                    sinks.tryEmitComplete();
                })
                .subscribe(
                        ignored -> {
                        },
                        err -> handleRoundError("强制总结异常", round.get(), hasSentFinalRes, sinks, err)
                );

        if(currentConversationId !=null && taskManager != null){
            taskManager.setDisposable(currentConversationId,disposable);
        }

    }

    private List<Message> sanitizeMessages(List<Message> messages) {
        List<Message> safeMessages = new ArrayList<>(messages.size());
        for (Message message : messages) {
            safeMessages.add(sanitizeMessage(message));
        }
        return safeMessages;
    }

    private void compactMessagesBeforeRequest(List<Message> messages) {
        if (contextCompactor == null || CollectionUtils.isEmpty(messages)) {
            return;
        }
        int beforeSize = messages.size();
        try {
            contextCompactor.compact(messages, currentQuestion);
            if (messages.size() != beforeSize) {
                log.info("上下文压缩完成: conversationId={}, beforeMessages={}, afterMessages={}",
                        currentConversationId, beforeSize, messages.size());
            }
        } catch (Exception e) {
            log.warn("上下文压缩失败，继续使用原始上下文: conversationId={}, messages={}, error={}",
                    currentConversationId, beforeSize, e.getMessage());
        }
    }

    private Message sanitizeMessage(Message message) {
        if (message instanceof AssistantMessage assistantMessage) {
            return AssistantMessage.builder()
                    .content(Objects.toString(assistantMessage.getText(), ""))
                    .properties(assistantMessage.getMetadata())
                    .toolCalls(sanitizeToolCalls(assistantMessage.getToolCalls()))
                    .media(assistantMessage.getMedia())
                    .build();
        }
        if (message instanceof ToolResponseMessage toolResponseMessage) {
            return ToolResponseMessage.builder()
                    .responses(sanitizeToolResponses(toolResponseMessage.getResponses()))
                    .metadata(toolResponseMessage.getMetadata())
                    .build();
        }
        return message;
    }

    private List<AssistantMessage.ToolCall> sanitizeToolCalls(List<AssistantMessage.ToolCall> toolCalls) {
        if (CollectionUtils.isEmpty(toolCalls)) {
            return List.of();
        }
        return toolCalls.stream()
                .map(toolCall -> new AssistantMessage.ToolCall(
                        Objects.toString(toolCall.id(), ""),
                        Objects.toString(toolCall.type(), "function"),
                        Objects.toString(toolCall.name(), ""),
                        Objects.toString(toolCall.arguments(), "")))
                .toList();
    }

    private List<ToolResponseMessage.ToolResponse> sanitizeToolResponses(List<ToolResponseMessage.ToolResponse> responses) {
        if (CollectionUtils.isEmpty(responses)) {
            return List.of();
        }
        return responses.stream()
                .map(response -> new ToolResponseMessage.ToolResponse(
                        Objects.toString(response.id(), ""),
                        Objects.toString(response.name(), ""),
                        Objects.toString(response.responseData(), "")))
                .toList();
    }

    private void initClient() {
        ToolCallingChatOptions.Builder toolCalling = ToolCallingChatOptions.builder();
        toolCalling
                .toolCallbacks(tools == null ? List.of() : tools);


        ChatClient.Builder builder = ChatClient.builder(chatModel);
        if (!CollectionUtils.isEmpty(advisors)) {
            builder.defaultAdvisors(advisors);
        }
        this.chatClient = builder
                .defaultOptions(toolCalling)
                .build();
        log.info("WebSearchReactAgent初始化完成: agent={}, tools={}, advisors={}, maxRounds={}",
                name, tools == null ? 0 : tools.size(), advisors == null ? 0 : advisors.size(), maxRounds);
    }

    private void handleRoundError(String message, int round, AtomicBoolean hasSentFinalRes,
                                  Sinks.Many<String> sinks, Throwable err) {
        logRoundSeparator(message, round);
        String errorMessage = err.getMessage();
        if (err instanceof WebClientResponseException webClientResponseException) {
            String responseBody = webClientResponseException.getResponseBodyAsString();
            if (StringUtils.isNotBlank(responseBody)) {
                errorMessage = responseBody;
            }
            log.error("{}: conversationId={}, round={}, status={}, responseBody={}",
                    message, currentConversationId, round, webClientResponseException.getStatusCode(),
                    abbreviate(responseBody, 2000), err);
        } else {
            log.error("{}: conversationId={}, round={}", message, currentConversationId, round, err);
        }
        hasSentFinalRes.set(true);
        sinks.tryEmitNext(createErrorResponse(errorMessage));
        sinks.tryEmitComplete();
    }

    private void logRoundSeparator(String stage, Integer round) {
        if (round == null) {
            log.info("==================== {} | conversationId={} ====================",
                    stage, currentConversationId);
            return;
        }
        log.info("==================== {} | conversationId={} | round={} ====================",
                stage, currentConversationId, round);
    }

    private String describeToolCalls(List<AssistantMessage.ToolCall> toolCalls) {
        if (CollectionUtils.isEmpty(toolCalls)) {
            return "[]";
        }
        return toolCalls.stream()
                .map(toolCall -> "%s(%s) args=%s".formatted(
                        toolCall.name(),
                        toolCall.id(),
                        abbreviate(toolCall.arguments(), 300)))
                .toList()
                .toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String name = "webSearchAgent";
        private String agentType = "websearch";
        private ChatModel chatModel;
        private ChatMemory chatMemory;
        private String systemPrompt = "";
        private List<ToolCallback> tools = List.of();
        private int maxRounds = 5;
        private List<Advisor> advisors = List.of();
        private ContextPolicy contextPolicy = ContextPolicy.defaults();
        private AgentTaskManager taskManager;
        private SessionAdapter sessionAdapter;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder agentType(String agentType) {
            this.agentType = agentType;
            return this;
        }

        public Builder chatModel(ChatModel chatModel) {
            this.chatModel = chatModel;
            return this;
        }

        public Builder chatMemory(ChatMemory chatMemory) {
            this.chatMemory = chatMemory;
            return this;
        }

        public Builder systemPrompt(String systemPrompt) {
            this.systemPrompt = systemPrompt;
            return this;
        }

        public Builder taskManager(AgentTaskManager taskManager) {
            this.taskManager = taskManager;
            return this;
        }

        public Builder aiSessionService(SessionAdapter sessionAdapter) {
            this.sessionAdapter = sessionAdapter;
            return this;
        }


        public Builder tools(List<ToolCallback> tools) {
            this.tools = tools == null ? List.of() : tools;
            return this;
        }

        public Builder tools(ToolCallback... tools) {
            this.tools = tools == null ? List.of() : List.of(tools);
            return this;
        }

        public Builder maxRounds(int maxRounds) {
            this.maxRounds = maxRounds;
            return this;
        }

        public Builder advisors(List<Advisor> advisors) {
            this.advisors = advisors == null ? List.of() : advisors;
            return this;
        }

        public Builder advisors(Advisor... advisors) {
            this.advisors = advisors == null ? List.of() : List.of(advisors);
            return this;
        }

        public Builder contextPolicy(ContextPolicy contextPolicy) {
            this.contextPolicy = contextPolicy == null ? ContextPolicy.defaults() : contextPolicy;
            return this;
        }

        public WebSearchReactAgent build() {
            if (chatModel == null) {
                throw new IllegalArgumentException("chatModel 不能为空");
            }
            return new WebSearchReactAgent(name, agentType, chatModel, chatMemory, systemPrompt, tools, maxRounds, taskManager,sessionAdapter,advisors, contextPolicy);
        }
    }
}

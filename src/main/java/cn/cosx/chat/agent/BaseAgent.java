package cn.cosx.chat.agent;

import cn.cosx.chat.adapter.SessionAdapter;
import cn.cosx.chat.common.enums.MessageTypeEnums;
import cn.cosx.chat.common.response.AgentResponse;
import cn.cosx.chat.entity.ChatMessage;
import cn.cosx.chat.entity.dto.AgentRoundSearchResult;
import cn.cosx.chat.entity.record.ChatRequest;
import cn.cosx.chat.entity.record.ChunkSegment;
import cn.cosx.chat.entity.record.SearchResult;
import cn.cosx.chat.prompt.ReactAgentPrompts;
import cn.cosx.chat.service.AgentTaskManager;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.memory.MessageWindowChatMemory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.converter.BeanOutputConverter;
import org.springframework.ai.deepseek.DeepSeekAssistantMessage;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.*;

@Slf4j
public abstract class BaseAgent {

    protected ChatModel chatModel;

    protected String name;

    protected ChatMemory chatMemory;

    protected String agentType;


    protected AgentTaskManager taskManager;

    protected SessionAdapter sessionAdapter;

    protected boolean enableRecommendations = true;

    protected String currentRecommendations;

    protected String currentConversationId;

    protected String currentQuestion;

    protected Set<String> usedTools = new HashSet<>();


    public BaseAgent(String name, String agentType, ChatModel chatModel) {
        this.name = name;
        this.agentType = agentType;
        this.chatModel = chatModel;
    }


    public abstract Flux<String> execute(ChatRequest request);

    public void setChatMemory(ChatMemory chatMemory){
        this.chatMemory = chatMemory;
    }



    protected AgentTaskManager.TaskInfo registerTask(String conversationId, Sinks.Many<String> sinks) {

        AgentTaskManager.TaskInfo taskInfo = taskManager.registerTask(conversationId, sinks);

        return taskInfo;
    }

    public Flux<String> checkTaskRunning(String conversationId){
        if(Objects.nonNull(conversationId) && taskManager.hasTask(conversationId)){
            return Flux.error(new IllegalStateException("该会话正在执行中，请稍后再试"));
        }
        return null;
    }
    public ChatMemory createPersistMessages(String conversationId,int maxCount){
        List<ChatMessage> historyMessages= sessionAdapter.getMessageByConversationId(conversationId,"cosx");
        MessageWindowChatMemory messageWindowChatMemory = MessageWindowChatMemory.builder().maxMessages(maxCount).build();


        if(!CollectionUtils.isEmpty(historyMessages)){
            for (ChatMessage message : historyMessages) {
                MessageTypeEnums messageType = message.getMessageType();
                if(messageType.equals(MessageTypeEnums.USER)){
                    messageWindowChatMemory.add(conversationId,new UserMessage(message.getConvertedContent()));
                }

                if(messageType.equals(MessageTypeEnums.ASSISTANT)){
                    messageWindowChatMemory.add(conversationId,new AssistantMessage(message.getContent()));
                }
            }
            log.info("加载会话历史数量：{}",historyMessages.size());
        }
        return messageWindowChatMemory;

    }

    protected void loadHistoryMessages(String conversationId, List<Message> messages, boolean skipSys, boolean label) {
        if(conversationId != null && chatMemory !=null){
            List<Message> history = chatMemory.get(conversationId);
            if(!CollectionUtils.isEmpty(history)){
                if( label){
                    messages.add(new UserMessage("对话历史："));
                }
                for (Message message : history) {
                    if(skipSys && message instanceof SystemMessage) continue;

                    messages.add(message);
                }
            }
        }
    }


    protected String generateRecommendations(String finalAnswer) {


            if (!enableRecommendations) {
                return null;
            }

            try {
                List<Message> messages = new ArrayList<>();

                // 1. 添加系统提示词
                messages.add(new SystemMessage(ReactAgentPrompts.getRecommendPrompt()));

                // 2. 添加历史消息
                loadHistoryMessages(currentConversationId, messages, true, true);

                // 3. 添加当前会话的消息（最新的消息，放在最后）
                messages.add(new UserMessage("当前会话："));
                messages.add(new UserMessage(currentQuestion));
                if (finalAnswer != null) {
                    messages.add(new AssistantMessage(finalAnswer));
                }

                // 4. 添加格式说明消息
                // 使用 BeanOutputConverter 进行结构化输出
                BeanOutputConverter<List<String>> converter = new BeanOutputConverter<>(new ParameterizedTypeReference<>() {
                });

                // 添加格式说明消息
                messages.add(new UserMessage("请根据上述对话生成3个推荐问题。输出格式为：\n" + converter.getFormat()));

                // 5. 调用模型生成推荐问题
                String response = ChatClient.builder(chatModel).build()
                        .prompt()
                        .messages(messages)
                        .call()
                        .content();
                log.info("推荐问题模型调用完成: conversationId={}, response={}",
                        currentConversationId, abbreviate(response, 500));

                // 6. 使用 converter 转换响应
                if (response != null && !response.isEmpty()) {
                    List<String> recommendations = converter.convert(response);
                    if (recommendations != null && !recommendations.isEmpty()) {
                        String jsonStr = JSON.toJSONString(recommendations);
                        log.info("生成推荐问题成功: {}", jsonStr);
                        return jsonStr;
                    }
                }

                log.warn("生成推荐问题失败，响应格式无效: {}", response);
                return null;
            } catch (Exception e) {
                log.error("生成推荐问题异常", e);
                return null;
            }

    }

    protected String abbreviate(String value, int maxLength) {
        if (value == null) {
            return null;
        }
        if (value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength) + "...(len=" + value.length() + ")";
    }

    protected List<ChunkSegment> parseChunk(ChatResponse chunk,Sinks.Many<String> sinks) {
        if(chunk == null || chunk.getResult() == null){
            return new ArrayList<>();
        }

        List<Generation> results = chunk.getResults();
        List<ChunkSegment> chunkSegments = new ArrayList<>();
        for (Generation result : results) {
            if(result == null || result.getOutput() == null){
                continue ;
            }

            AssistantMessage assistantMessage = result.getOutput();
            if(assistantMessage instanceof DeepSeekAssistantMessage deepSeekAssistantMessage){
                String reasoningContent = deepSeekAssistantMessage.getReasoningContent();
                String text = deepSeekAssistantMessage.getText();
                if(reasoningContent != null){
                    ChunkSegment chunkSegment = new ChunkSegment(reasoningContent,true);
                    chunkSegments.add(chunkSegment);
                    sinks.tryEmitNext(createThinkingResponse(reasoningContent));

                }else if(text != null){
                    ChunkSegment chunkSegment = new ChunkSegment(text,false);
                    chunkSegments.add(chunkSegment);
                    sinks.tryEmitNext(createTextResponse(text));

                }
            }
        }

        return chunkSegments;
    }

    protected void parseTavilyResult(String result, AgentRoundSearchResult agentRound) {
        try {
            JSONArray jsonArray = JSON.parseArray(result);
            if(jsonArray == null || jsonArray.isEmpty()){
                return ;
            }
            JSONObject jsonItem = jsonArray.getJSONObject(0);
            if(jsonItem == null){
                return ;
            }

            Object textValue = jsonItem.get("text");
            if(textValue == null){
                return ;
            }

            JSONObject tavilyObject;
            if(textValue instanceof JSONObject textObject){
                tavilyObject = textObject;
            }else{
                tavilyObject = JSON.parseObject(String.valueOf(textValue));
            }

            if(tavilyObject == null){
                return ;
            }

            JSONArray results = tavilyObject.getJSONArray("results");

            if(results == null || results.isEmpty()){
                return ;
            }
            List<SearchResult> searchResults = agentRound.searchResults;
            for (int i = 0; i < results.size(); i++) {
                JSONObject item = results.getJSONObject(i);
                if(item == null){
                    continue;
                }
                String url = item.getString("url");
                if(url == null || url.isBlank()){
                    continue;
                }
                String content = item.getString("content");
                String title = item.getString("title");

                SearchResult searchResult = new SearchResult(url, title, content);
                searchResults.add(searchResult);
            }
        } catch (Exception e) {
            log.warn("解析 tavily 搜索结果失败: {}", e.getMessage());
        }
    }


    /**
     * 创建Agent响应
     *
     * @param content 内容
     * @param type    类型
     * @return JSON格式的响应字符串
     */
    protected String createResponse(String content, String type) {
        return AgentResponse.json(type, content);
    }

    /**
     * 创建text类型响应
     *
     * @param content 内容
     * @return JSON格式的响应字符串
     */
    protected String createTextResponse(String content) {
        return AgentResponse.text(content);
    }

    /**
     * 创建thinking类型响应
     *
     * @param content 内容
     * @return JSON格式的响应字符串
     */
    protected String createThinkingResponse(String content) {
        return AgentResponse.thinking(content);
    }

    /**
     * 创建reference类型响应
     *
     * @param content 内容（JSON数组字符串，count会自动计算）
     * @return JSON格式的响应字符串
     */
    protected String createReferenceResponse(String content) {
        return AgentResponse.reference(content);
    }

    /**
     * 创建error类型响应
     *
     * @param content 内容
     * @return JSON格式的响应字符串
     */
    protected String createErrorResponse(String content) {
        return AgentResponse.error(content);
    }

    /**
     * 创建recommend类型响应
     *
     * @param content 内容（推荐问题JSON数组字符串）
     * @return JSON格式的响应字符串
     */
    protected String createRecommendResponse(String content) {
        return AgentResponse.recommend(content);
    }

}

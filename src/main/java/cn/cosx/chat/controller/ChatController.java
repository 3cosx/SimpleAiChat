package cn.cosx.chat.controller;

import cn.cosx.chat.adapter.SessionAdapter;
import cn.cosx.chat.agent.web.WebSearchReactAgent;
import cn.cosx.chat.entity.record.ChatRequest;
import cn.cosx.chat.service.AgentTaskManager;
import cn.cosx.chat.service.TitleSummaryService;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.mcp.SyncMcpToolCallbackProvider;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/chat")
@RequiredArgsConstructor
@Slf4j
public class ChatController implements InitializingBean {

    private static final String DEFAULT_TAVILY_MCP_URL = "https://mcp.tavily.com/mcp/";

    private final ChatMemory chatMemory;
    private final ChatModel chatModel;
    private final AgentTaskManager taskManager;
    private final SessionAdapter sessionAdapter;
    private final TitleSummaryService titleSummaryService;
    private List<ToolCallback> webSearchToolCallbacks = List.of();
    private McpSyncClient tavilyMcpClient;


    /**
     * Tavily 搜索引擎 API Key
     */
    @Value("${tavily.api-key}")
    private String tavilyApiKey;

    /**
     * Tavily MCP URL
     */
    @Value("${tavily.mcp-url:" + DEFAULT_TAVILY_MCP_URL + "}")
    private String tavilyMcpUrl;


    @GetMapping(value = "/stream", produces = "text/event-stream;charset=UTF-8")
    public Flux<String> webSearchStream(@RequestParam("query") String query,
                                        @RequestParam(value = "conversationId", required = false) String conversationId) {
        log.info("收到网页搜索请求: query={}, conversationId={}", query, conversationId);

        if (query == null || query.trim().isEmpty()) {
            return Flux.error(new IllegalArgumentException("查询参数不能为空"));
        }

        WebSearchReactAgent webSearchReactAgent = initWebSearchAgent();
        String effectiveConversationId = conversationId;
        if(!StringUtils.hasText(effectiveConversationId)){
            effectiveConversationId = sessionAdapter.saveConversation(query);
            final String titleConversationId = effectiveConversationId;
            Thread.ofVirtual().name("titleSummary" + titleConversationId).start(() -> {
                String title = titleSummaryService.generateTitle(query);
                sessionAdapter.updateTitle(titleConversationId, title);
                log.info("标题更新成功: conversationId={}, title={}", titleConversationId, title);
            });
        }

        //加载记忆
        ChatMemory chatMemory = webSearchReactAgent.createPersistMessages(effectiveConversationId, 30);
        webSearchReactAgent.setChatMemory(chatMemory);
        return webSearchReactAgent.execute(new ChatRequest(effectiveConversationId, query));
    }

    private WebSearchReactAgent initWebSearchAgent() {
        return WebSearchReactAgent.builder()
                .chatModel(chatModel)
                .chatMemory(chatMemory)
                .aiSessionService(sessionAdapter)
                .taskManager(taskManager)
                .tools(webSearchToolCallbacks)
                .maxRounds(5)
                .build();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        initWebSearchToolCallbacks();
    }

    /**
     * 初始化网页搜索工具回调
     */
    private void initWebSearchToolCallbacks() throws Exception {
        log.info("初始化网页搜索工具回调...");

        if (!StringUtils.hasText(tavilyApiKey)) {
            throw new IllegalStateException("tavily.api-key 不能为空，无法初始化 Tavily MCP 工具");
        }
        if (!StringUtils.hasText(tavilyMcpUrl)) {
            throw new IllegalStateException("tavily.mcp-url 不能为空，无法初始化 Tavily MCP 工具");
        }

        try {
            String authorizationHeader = "Bearer " + tavilyApiKey;
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .header("Authorization", authorizationHeader);

            HttpClientStreamableHttpTransport tavTransport = HttpClientStreamableHttpTransport.builder(tavilyMcpUrl)
                    .requestBuilder(requestBuilder)
                    .build();

            tavilyMcpClient = McpClient.sync(tavTransport)
                    .requestTimeout(Duration.ofSeconds(300))
                    .build();
            tavilyMcpClient.initialize();

            List<McpSyncClient> mcpClients = List.of(tavilyMcpClient);
            SyncMcpToolCallbackProvider provider = SyncMcpToolCallbackProvider.builder()
                    .mcpClients(mcpClients)
                    .build();
            webSearchToolCallbacks = Arrays.asList(provider.getToolCallbacks());

            String toolNames = webSearchToolCallbacks.stream()
                    .map(toolCallback -> toolCallback.getToolDefinition().name())
                    .collect(Collectors.joining(", "));
            log.info("网页搜索工具回调初始化完成，工具数量: {}, tools={}",
                    webSearchToolCallbacks.size(), toolNames);
        } catch (Exception ex) {
            closeTavilyMcpClient();
            webSearchToolCallbacks = List.of();
            log.error("初始化 Tavily MCP 工具失败，网页搜索工具将暂不可用", ex);
        }
    }

    @PreDestroy
    public void closeTavilyMcpClient() {
        if (tavilyMcpClient != null) {
            tavilyMcpClient.close();
            tavilyMcpClient = null;
        }
    }
}

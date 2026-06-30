package cn.cosx.chat.controller;

import com.alibaba.fastjson2.JSON;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.mcp.SyncMcpToolCallbackProvider;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.definition.ToolDefinition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/web/test")
@RequiredArgsConstructor
@Slf4j
public class TestController {

    private static final String DEFAULT_TAVILY_MCP_URL = "https://mcp.tavily.com/mcp/";

    private final ChatModel chatModel;
    private volatile List<ToolCallback> tavilyToolCallbacks;
    private volatile McpSyncClient tavilyMcpClient;

    @Value("${tavily.api-key}")
    private String tavilyApiKey;

    @Value("${tavily.mcp-url:" + DEFAULT_TAVILY_MCP_URL + "}")
    private String tavilyMcpUrl;

    @GetMapping("/chat-model")
    public Mono<ChatResponse> chatModel(@RequestParam String query) {
        return Mono.fromCallable(() -> chatModel.call(new Prompt(query)))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping(value = "/chat-model/stream", produces = "text/event-stream;charset=UTF-8")
    public Flux<ChatResponse> streamChatModel(@RequestParam String query) {
        return chatModel.stream(new Prompt(query));
    }

    @GetMapping("/tavily-mcp/tools")
    public Mono<List<ToolInfo>> tavilyMcpTools() {
        return Mono.fromCallable(() -> ensureTavilyToolCallbacks().stream()
                        .map(ToolInfo::from)
                        .toList())
                .subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping("/tavily-mcp/search")
    public Mono<String> tavilyMcpSearch(@RequestParam String query) {
        return Mono.fromCallable(() -> {
                    ToolCallback searchTool = ensureTavilyToolCallbacks().stream()
                            .filter(tool -> tool.getToolDefinition().name().toLowerCase().contains("search"))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException("未找到 Tavily search 工具"));
                    String arguments = JSON.toJSONString(Map.of("query", query));
                    String result = searchTool.call(arguments);
                    log.info("tavily调用结果：{}",result);
                    return result;
                })
                .onErrorResume(ex -> Mono.just("Tavily MCP 调用失败: " + rootMessage(ex)))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @PostMapping("/tavily-mcp/call")
    public Mono<String> tavilyMcpCall(@RequestParam String toolName,
                                      @RequestBody(required = false) String arguments) {
        return Mono.fromCallable(() -> {
                    ToolCallback toolCallback = ensureTavilyToolCallbacks().stream()
                            .filter(tool -> tool.getToolDefinition().name().equals(toolName))
                            .findFirst()
                            .orElseThrow(() -> new IllegalArgumentException("未找到工具: " + toolName));
                    return toolCallback.call(StringUtils.hasText(arguments) ? arguments : "{}");
                })
                .onErrorResume(ex -> Mono.just("Tavily MCP 调用失败: " + rootMessage(ex)))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private List<ToolCallback> ensureTavilyToolCallbacks() {
        if (tavilyToolCallbacks != null) {
            return tavilyToolCallbacks;
        }
        synchronized (this) {
            if (tavilyToolCallbacks != null) {
                return tavilyToolCallbacks;
            }

            if (!StringUtils.hasText(tavilyApiKey)) {
                throw new IllegalStateException("tavily.api-key 不能为空，无法初始化 Tavily MCP 工具");
            }
            if (!StringUtils.hasText(tavilyMcpUrl)) {
                throw new IllegalStateException("tavily.mcp-url 不能为空，无法初始化 Tavily MCP 工具");
            }

            TavilyMcpAddress address = buildTavilyMcpAddress();
            String authorizationHeader = "Bearer " + tavilyApiKey;
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .header("Authorization", authorizationHeader);

            HttpClientStreamableHttpTransport transport = HttpClientStreamableHttpTransport.builder(address.baseUrl())
                    .endpoint(address.endpoint())
                    .clientBuilder(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(60)))
                    .connectTimeout(Duration.ofSeconds(60))
                    .requestBuilder(requestBuilder)
                    .build();

            tavilyMcpClient = McpClient.sync(transport)
                    .requestTimeout(Duration.ofSeconds(300))
                    .build();
            tavilyMcpClient.initialize();

            SyncMcpToolCallbackProvider provider = SyncMcpToolCallbackProvider.builder()
                    .mcpClients(tavilyMcpClient)
                    .build();
            tavilyToolCallbacks = Arrays.asList(provider.getToolCallbacks());
            return tavilyToolCallbacks;
        }
    }

    private TavilyMcpAddress buildTavilyMcpAddress() {
        URI uri = URI.create(tavilyMcpUrl.trim());
        String baseUrl = uri.getScheme() + "://" + uri.getAuthority();
        String path = StringUtils.hasText(uri.getRawPath()) && !"/".equals(uri.getRawPath())
                ? uri.getRawPath()
                : "/mcp/";
        String query = uri.getRawQuery();

        if (!StringUtils.hasText(query) || !query.contains("tavilyApiKey=")) {
            String encodedApiKey = URLEncoder.encode(tavilyApiKey, StandardCharsets.UTF_8);
            query = StringUtils.hasText(query)
                    ? query + "&tavilyApiKey=" + encodedApiKey
                    : "tavilyApiKey=" + encodedApiKey;
        }

        return new TavilyMcpAddress(baseUrl, path + "?" + query);
    }

    private String rootMessage(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }

    @PreDestroy
    public void closeTavilyMcpClient() {
        if (tavilyMcpClient != null) {
            tavilyMcpClient.close();
            tavilyMcpClient = null;
        }
    }

    public record ToolInfo(String name, String description, String inputSchema) {

        static ToolInfo from(ToolCallback toolCallback) {
            ToolDefinition definition = toolCallback.getToolDefinition();
            return new ToolInfo(definition.name(), definition.description(), definition.inputSchema());
        }
    }

    private record TavilyMcpAddress(String baseUrl, String endpoint) {
    }
}

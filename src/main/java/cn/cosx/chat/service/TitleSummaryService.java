package cn.cosx.chat.service;

import cn.cosx.chat.entity.ChatConversation;
import lombok.RequiredArgsConstructor;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.deepseek.DeepSeekChatOptions;
import org.springframework.ai.deepseek.api.DeepSeekApi;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
@RequiredArgsConstructor
public class TitleSummaryService {

    private final ChatModel chatModel;
    private final ChatConversationService chatConversationService;

    public String generateTitle(String query) {
        if (!StringUtils.hasText(query)) {
            return "新会话";
        }

        return ChatClient.builder(chatModel)
                .build()
                .prompt()
                .options(DeepSeekChatOptions.builder()
                        .model(DeepSeekApi.ChatModel.DEEPSEEK_V4_FLASH)
                        .temperature(0.3)
                        .maxTokens(30))
                .system("""
                        你是会话标题生成器。根据用户问题生成一个简短中文标题。
                        只输出标题，不要解释，不要标点包裹，最长 15 个字。
                        """)
                .user(query)
                .call()
                .content();
    }


}

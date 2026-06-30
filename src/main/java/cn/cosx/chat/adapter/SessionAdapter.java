package cn.cosx.chat.adapter;

import cn.cosx.chat.common.enums.MessageTypeEnums;
import cn.cosx.chat.entity.ChatConversation;
import cn.cosx.chat.entity.ChatMessage;
import cn.cosx.chat.entity.record.SearchResult;
import cn.cosx.chat.entity.request.ConversationDetailRequest;
import cn.cosx.chat.entity.request.ConversationListRequest;
import cn.cosx.chat.entity.request.ConversationMessageListRequest;
import cn.cosx.chat.service.ChatConversationService;
import cn.cosx.chat.service.ChatMessageService;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.*;

@Component
@RequiredArgsConstructor
public class SessionAdapter {

    private final ChatMessageService chatMessageService;

    private final ChatConversationService chatConversationService;

    @Value("${spring.ai.deepseek.chat.model}")
    private String modelName;

    private static final String DEFAULT_USER_ID = "cosx";
    private static final long DEFAULT_CURRENT = 1L;
    private static final long DEFAULT_CONVERSATION_SIZE = 20L;
    private static final long DEFAULT_MESSAGE_SIZE = 50L;
    private static final long MAX_PAGE_SIZE = 100L;

    public IPage<ChatConversation> pageConversations(ConversationListRequest request) {
        ConversationListRequest safeRequest = request == null ? new ConversationListRequest() : request;
        String userId = defaultUserId(safeRequest.getUserId());
        long current = defaultCurrent(safeRequest.getCurrent());
        long size = defaultSize(safeRequest.getSize(), DEFAULT_CONVERSATION_SIZE);

        return chatConversationService.lambdaQuery()
                .eq(ChatConversation::getUserId, userId)
                .like(StringUtils.hasText(safeRequest.getKeyword()), ChatConversation::getTitle, safeRequest.getKeyword())
                .orderByDesc(ChatConversation::getUpdatedTime)
                .orderByDesc(ChatConversation::getCreatedTime)
                .page(Page.of(current, size));
    }

    public ChatConversation getConversationDetail(ConversationDetailRequest request) {
        if (request == null || !StringUtils.hasText(request.getConversationId())) {
            return null;
        }
        String userId = defaultUserId(request.getUserId());
        return chatConversationService.lambdaQuery()
                .eq(ChatConversation::getConversationId, request.getConversationId())
                .eq(ChatConversation::getUserId, userId)
                .one();
    }

    public IPage<ChatMessage> pageConversationMessages(ConversationMessageListRequest request) {
        ConversationMessageListRequest safeRequest = request == null ? new ConversationMessageListRequest() : request;
        long current = defaultCurrent(safeRequest.getCurrent());
        long size = defaultSize(safeRequest.getSize(), DEFAULT_MESSAGE_SIZE);
        Page<ChatMessage> emptyPage = Page.of(current, size);

        if (!StringUtils.hasText(safeRequest.getConversationId())) {
            return emptyPage;
        }

        String userId = defaultUserId(safeRequest.getUserId());
        return chatMessageService.lambdaQuery()
                .eq(ChatMessage::getConversationId, safeRequest.getConversationId())
                .eq(ChatMessage::getUserId, userId)
                .orderByAsc(ChatMessage::getCreatedTime)
                .page(emptyPage);
    }

    public List<ChatMessage> getMessageByConversationId(String conversationId,String userId) {
        List<ChatMessage> messages = chatMessageService.lambdaQuery()
                .eq(ChatMessage::getConversationId, conversationId)
                .eq(ChatMessage::getUserId, defaultUserId(userId))
                .orderByDesc(ChatMessage::getCreatedTime).list();
        if(CollectionUtils.isEmpty(messages)){
            return Collections.emptyList();
        }
        return messages;
    }

    public String saveConversation(String query) {
        String conversationId = UUID.randomUUID().toString();
        ChatConversation chatConversation = new ChatConversation();
        chatConversation.setConversationId(conversationId);
        chatConversation.setTitle(buildDefaultTitle(query));
        chatConversation.setUserId("cosx");
        chatConversationService.save(chatConversation);
        return conversationId;
    }

    public void updateTitle(String conversationId, String title) {
        if (!StringUtils.hasText(conversationId) || !StringUtils.hasText(title)) {
            return;
        }

        chatConversationService.lambdaUpdate()
                .eq(ChatConversation::getConversationId, conversationId)
                .set(ChatConversation::getTitle, title)
                .update();
    }

    public void saveUserMessage(String conversationId, String query) {
        ChatMessage chatMessage = new ChatMessage();
        chatMessage.setContent(query);
        chatMessage.setConversationId(conversationId);
        chatMessage.setConvertedContent(query);
        chatMessage.setMessageType(MessageTypeEnums.USER);
        chatMessageService.save(chatMessage);
    }

    public void saveAssisantMessage(String conversationId, String finalAnswer, String thinking,
                                    Set<String> usedTools, String currentRecommendations, List<SearchResult> searchResults) {
        ChatMessage chatMessage = new ChatMessage();
        chatMessage.setContent(finalAnswer);
        chatMessage.setConversationId(conversationId);
        chatMessage.setConvertedContent(finalAnswer);
        if(StringUtils.hasText(thinking)){
            chatMessage.setThinkingContent(thinking);
        }
        if(!CollectionUtils.isEmpty(usedTools)){
            chatMessage.setUsedTools(new ArrayList<>(usedTools));
        }
        chatMessage.setRecommendations(currentRecommendations);
        chatMessage.setMessageType(MessageTypeEnums.ASSISTANT);
        chatMessage.setToolCallingResult(JSON.toJSONString(searchResults));
        chatMessageService.save(chatMessage);
    }

    private String defaultUserId(String userId) {
        return StringUtils.hasText(userId) ? userId : DEFAULT_USER_ID;
    }

    private String buildDefaultTitle(String query) {
        if (!StringUtils.hasText(query)) {
            return "新会话";
        }
        String trimmed = query.trim();
        return trimmed.length() <= 20 ? trimmed : trimmed.substring(0, 20);
    }

    private long defaultCurrent(Long current) {
        return current == null || current < 1 ? DEFAULT_CURRENT : current;
    }

    private long defaultSize(Long size, long defaultSize) {
        if (size == null || size < 1) {
            return defaultSize;
        }
        return Math.min(size, MAX_PAGE_SIZE);
    }
}

package cn.cosx.chat.entity.request;

import lombok.Data;

@Data
public class ConversationMessageListRequest {

    private String conversationId;

    private String userId = "cosx";

    private Long current = 1L;

    private Long size = 50L;
}

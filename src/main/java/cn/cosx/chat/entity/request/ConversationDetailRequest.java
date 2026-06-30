package cn.cosx.chat.entity.request;

import lombok.Data;

@Data
public class ConversationDetailRequest {

    private String conversationId;

    private String userId = "cosx";
}

package cn.cosx.chat.entity.request;

import lombok.Data;

@Data
public class ConversationListRequest {

    private String userId = "cosx";

    private Long current = 1L;

    private Long size = 20L;

    private String keyword;
}

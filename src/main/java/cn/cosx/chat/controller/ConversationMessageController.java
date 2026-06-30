package cn.cosx.chat.controller;

import cn.cosx.chat.adapter.SessionAdapter;
import cn.cosx.chat.entity.ChatMessage;
import cn.cosx.chat.entity.request.ConversationMessageListRequest;
import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/messages")
@RequiredArgsConstructor
public class ConversationMessageController {

    private final SessionAdapter sessionAdapter;

    @GetMapping("/list")
    public IPage<ChatMessage> list(ConversationMessageListRequest request) {
        return sessionAdapter.pageConversationMessages(request);
    }
}

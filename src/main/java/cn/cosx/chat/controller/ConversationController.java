package cn.cosx.chat.controller;

import cn.cosx.chat.adapter.SessionAdapter;
import cn.cosx.chat.entity.ChatConversation;
import cn.cosx.chat.entity.request.ConversationDetailRequest;
import cn.cosx.chat.entity.request.ConversationListRequest;
import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/conversations")
@RequiredArgsConstructor
public class ConversationController {

    private final SessionAdapter sessionAdapter;

    @GetMapping("/list")
    public IPage<ChatConversation> list(ConversationListRequest request) {
        return sessionAdapter.pageConversations(request);
    }

    @GetMapping("/detail")
    public ChatConversation detail(ConversationDetailRequest request) {
        return sessionAdapter.getConversationDetail(request);
    }
}

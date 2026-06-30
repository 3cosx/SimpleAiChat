package cn.cosx.chat.service.impl;

import cn.cosx.chat.entity.ChatConversation;
import cn.cosx.chat.mapper.ChatConversationMapper;
import cn.cosx.chat.service.ChatConversationService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

@Service
public class ChatConversationServiceImpl extends ServiceImpl<ChatConversationMapper, ChatConversation>
        implements ChatConversationService {
}

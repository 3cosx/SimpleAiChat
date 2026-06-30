package cn.cosx.chat.service.impl;

import cn.cosx.chat.entity.ChatMessage;
import cn.cosx.chat.mapper.ChatMessageMapper;
import cn.cosx.chat.service.ChatMessageService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

@Service
public class ChatMessageServiceImpl extends ServiceImpl<ChatMessageMapper, ChatMessage>
        implements ChatMessageService {
}

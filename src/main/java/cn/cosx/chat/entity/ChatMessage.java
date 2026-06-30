package cn.cosx.chat.entity;

import cn.cosx.chat.common.enums.MessageTypeEnums;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.handlers.Fastjson2TypeHandler;
import lombok.Data;

import java.util.List;

@Data
@TableName(value = "chat_message", autoResultMap = true)
public class ChatMessage extends BaseEntity{

    private Long id ;

    private String conversationId;

    private MessageTypeEnums messageType;

    private String content;

    private String modelName;

    private String convertedContent;

    private Integer tokens;

    private String userId;

    @TableField(typeHandler = Fastjson2TypeHandler.class)
    private List<String> usedTools;

    private String toolCallingResult;

    private String recommendations;

    private String thinkingContent;

}

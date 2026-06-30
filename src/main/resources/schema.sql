-- MySQL 8.x DDL for ChatConversation and ChatMessage entities.
-- Column names follow MyBatis-Plus underscore mapping from Java fields.

CREATE TABLE IF NOT EXISTS `chat_conversation` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `conversation_id` VARCHAR(64) NOT NULL COMMENT '会话唯一标识',
    `user_id` VARCHAR(64) NOT NULL COMMENT '用户ID',
    `title` VARCHAR(255) DEFAULT NULL COMMENT '会话标题',
    `created_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    `lock_version` INT NOT NULL DEFAULT 0 COMMENT '乐观锁版本号',
    `deleted` TINYINT NOT NULL DEFAULT 0 COMMENT '是否删除：0-未删除，1-已删除',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_chat_conversation_conversation_id` (`conversation_id`),
    KEY `idx_chat_conversation_user_updated` (`user_id`, `deleted`, `updated_time` DESC, `id` DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='聊天会话';

CREATE TABLE IF NOT EXISTS `chat_message` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `conversation_id` VARCHAR(64) NOT NULL COMMENT '会话唯一标识',
    `message_type` VARCHAR(32) NOT NULL COMMENT '消息类型',
    `content` LONGTEXT COMMENT '消息内容',
    `model_name` VARCHAR(128) DEFAULT NULL COMMENT '模型名称',
    `converted_content` LONGTEXT COMMENT '转换后的内容',
    `tokens` INT DEFAULT NULL COMMENT 'Token 数量',
    `user_id` VARCHAR(64) DEFAULT NULL COMMENT '用户ID',
    `used_tools` JSON DEFAULT NULL COMMENT '使用的工具列表',
    `tool_calling_result` LONGTEXT COMMENT '工具调用结果',
    `recommendations` LONGTEXT COMMENT '推荐内容',
    `thinking_content` LONGTEXT COMMENT '思考内容',
    `created_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    `lock_version` INT NOT NULL DEFAULT 0 COMMENT '乐观锁版本号',
    `deleted` TINYINT NOT NULL DEFAULT 0 COMMENT '是否删除：0-未删除，1-已删除',
    PRIMARY KEY (`id`),
    KEY `idx_chat_message_conversation_created` (`conversation_id`, `deleted`, `created_time`, `id`),
    KEY `idx_chat_message_user_conversation_created` (`user_id`, `conversation_id`, `deleted`, `created_time`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='聊天消息';

-- Current title search uses LIKE '%keyword%'. A normal BTREE index cannot optimize
-- that pattern. If title keyword search becomes a hotspot, enable a FULLTEXT index
-- and change the query to MATCH(title) AGAINST(...), optionally with an ngram parser
-- for Chinese tokenization:
-- ALTER TABLE `chat_conversation`
--     ADD FULLTEXT KEY `ft_chat_conversation_title` (`title`);

package alibaba.weibo.domain;

import java.util.Date;

/**
 * 消息表
 */
@lombok.Setter
@lombok.Getter
public class Message {
    /**
     * 消息ID
     */
    private Long messageId;
    /**
     * 用户ID
     */
    private Long userId;
    /**
     * 消息正文
     */
        private String messageContent;
    /**
     * 消息创建时间
     */
    private Date messageCreateDate;

}

package alibaba.weibo.domain;


import lombok.Getter;
import lombok.Setter;

import java.util.Date;

/**
 * 用户关注表
 */
@Setter
@Getter
public class Attention {
    /**
     * 关注表主键ID
     */
    private Long attentionId;
    /**
     * 关注用户ID
     */
    private Long attentionUserId;
    /**
     * 被关注用户ID
     */
    private Long beAttentionUserId;
    /**
     * 关注时间
     */
    private Date attentionDate;

}

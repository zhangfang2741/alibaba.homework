package com.examination.zhangfang.weibo;

import com.examination.zhangfang.weibo.domain.Message;
import java.util.List;

/**
 * 2 设计一个简化版的微博
 *      用户能关注和解除关注另一个用户，并且用户能看到TA所关注用户的最新10条消息。画出设计图(如类图、ER图等)，并写一个类，包含如下方法：
 *      postMessage: 用户发布一条微博消息；
 *      listRecentMessage：用户查看TA所关注用户(包括TA自己)的最新10条消息；
 *      follow：用户关注另一个用户；
 *      unfollow：用户解除关注另一个用户。
 *
 */
public interface UserActivityService {
    /**
     * 用户发布一条微博消息
     * @param message
     */
    public void postMessage(Message message);

    /**
     * 用户查看TA所关注用户(包括TA自己)的最新10条消息
     * @param userId
     * @return
     */
    public List<Message> listRecentMessage(Long userId);

    /**
     * 用户关注另一个用户
     * @param attentionUserId
     * @param beAttentionUserId
     */
    public void follow(Long attentionUserId, Long beAttentionUserId);

    /**
     * 用户解除关注另一个用户
     * @param attentionUserId
     * @param beAttentionUserId
     */
    public void unfollow(Long attentionUserId, Long beAttentionUserId);
}

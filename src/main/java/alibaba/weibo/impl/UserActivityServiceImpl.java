package alibaba.weibo.impl;

import alibaba.weibo.UserActivityService;
import alibaba.weibo.domain.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class UserActivityServiceImpl implements UserActivityService {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserActivityServiceImpl.class);
    /**
     * 用户发布一条微博消息
     * @param message
     */
    public void postMessage(Message message) {
        //向消息表添加一条数据
    }
    /**
     * 用户查看TA所关注用户(包括TA自己)的最新10条消息
     * @param userId
     * @return
     */
    public List<Message> listRecentMessage(Long userId) {
        //1、通过userId从Attention表中查询处该userId所关注的用户集合
        //2、从Message表中按照userId分组查询出前10条消息
        return null;
    }
    /**
     * 用户关注另一个用户
     * @param attentionUserId
     * @param beAttentionUserId
     */
    public void follow(Long attentionUserId, Long beAttentionUserId) {
        //向关注表中插入一条数据
    }
    /**
     * 用户解除关注另一个用户
     * @param attentionUserId
     * @param beAttentionUserId
     */
    public void unfollow(Long attentionUserId, Long beAttentionUserId) {
        //从关注表中删除一条数据
    }
}

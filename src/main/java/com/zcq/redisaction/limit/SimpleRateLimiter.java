package com.zcq.redisaction.limit;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.IOException;

/**
 * 基于zset的简单限流器
 * @author Maybeeeee
 * @date 2019-08-07 23:59
 */
public class SimpleRateLimiter {
    private Jedis jedis;

    public SimpleRateLimiter(Jedis jedis) {
        this.jedis = jedis;
    }

    /**
     * 操作限流
     *
     * @param userId    用户id
     * @param actionKey 用户操作行为
     * @param period    限流时间
     * @param maxCount  限流量
     * @return
     */
    public boolean isActionAllowed(String userId, String actionKey, int period, int maxCount) {
        String key = String.format("hits:%s:%s", userId, actionKey);
        Long nowTs = System.currentTimeMillis();
        Pipeline pipeline = jedis.pipelined();
        pipeline.multi();
        pipeline.zadd(key, nowTs, "" + nowTs);
        //去除掉这个时间窗口之前的行为。留下的都是时间窗口内的
        pipeline.zremrangeByScore(key, 0, nowTs - period * 1000);
        //计算这个时间窗口内的数量
        Response<Long> count = pipeline.zcard(key);

        //设置过期时间，防止冷用户长期占用内存
        pipeline.expire(key, period + 1);
        pipeline.exec();
        try {
            pipeline.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //是否超标
        return count.get() <= maxCount;
    }

    public static void main(String[] args) {
        Jedis jedis = new Jedis();
        SimpleRateLimiter limiter = new SimpleRateLimiter(jedis);
        for(int i=0;i<20;i++) {
            System.out.println(limiter.isActionAllowed("zcq", "reply", 60, 5));
        }
    }
}

package com.zcq.redisaction.limit;

import com.alibaba.fastjson.JSON;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;


/**
 * 单机下基于漏斗算法的限流器
 * @author Maybeeeee
 * @date 2019-08-08 21:35
 */
public class FunnelRateLimiter {

    private Jedis jedis;

    public FunnelRateLimiter(Jedis jedis) {
        this.jedis = jedis;
    }

    static class Funnel {
        //漏斗容量
        int capacity;
        //漏嘴流水速率
        float leakingRate;
        //漏斗剩余空间
        int leftQuota;
        //上次漏水的时间
        long leakingTs;

        public Funnel(int capacity, float leakingRate) {
            this.capacity = capacity;
            this.leakingRate = leakingRate;
            this.leftQuota = capacity;
            this.leakingTs = System.currentTimeMillis();
        }

        //每次加水之前要漏水造容量
        void makeSpace() {
            long nowTs = System.currentTimeMillis();
            long deltaTs = nowTs - leakingTs;
            //漏水量
            int deltaQuota = (int) (deltaTs * leakingRate);
            // 间隔时间太长，整数数字过大溢出
            if (deltaQuota < 0) {
                this.leftQuota = capacity;
                this.leakingTs = nowTs;
                return;
            }
            // 腾出空间太小，最小单位是 1
            if (deltaQuota < 1) {
                return;
            }
            this.leftQuota += deltaQuota;
            this.leakingTs = nowTs;
            if (this.leftQuota > this.capacity) {
                this.leftQuota = this.capacity;
            }
        }

        //加水
        boolean watering(int quota) {
            makeSpace();
            if (this.leftQuota >= quota) {
                this.leftQuota -= quota;
                return true;
            }
            return false;
        }
    }

    /**
     * 分布式下将漏斗的对象内容存储到一个hash结构中，
     * 这样还是有问题，不能保证从hash中取和放以及赋值是原子操作，需要加锁...
     */
    public boolean isActionAllowed(String userId, String actionKey, int capacity, float leakingRate) {
        String key = String.format("%s:%s", userId, actionKey);
        String s = jedis.hget("funnelRatel", key);
        Funnel funnel = JSON.parseObject(s, Funnel.class);
        if (s == null) {
            funnel = new Funnel(capacity, leakingRate);
            jedis.hset("funnelRatel", key, JSON.toJSONString(funnel));
        }
        //加水1个
        return funnel.watering(1);

    }


    public static void main(String[] args){
        Jedis jedis = new Jedis("127.0.0.1", 6380);
        FunnelRateLimiter funnelRateLimiter = new FunnelRateLimiter(jedis);
        for (int i = 0; i < 20; i++) {
            funnelRateLimiter.isActionAllowed("zcq", "water", 10, 0.0001666667f);
        }

    }
}

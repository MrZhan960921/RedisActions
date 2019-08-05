package com.zcq.redisaction.ReentrantLock;

import org.springframework.transaction.annotation.Transactional;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * Redis分布式锁的可重入实现
 *
 * @author Maybeeeee
 * @date 2019-08-05 21:51
 */
public class RedisWithReentrantLock {

    private ThreadLocal<Map> lockers = new ThreadLocal<>();

    private Jedis jedis;

    public RedisWithReentrantLock(Jedis jedis) {
        this.jedis = jedis;
    }

    /**
     * 加锁
     */
    private Boolean lock(String key) {
        return jedis.set(key, "", "nx", "ex", 5) != null;
    }

    /**
     * 解锁
     */
    private void unlock(String key) {
        jedis.del(key);
    }

    /**
     * 获取当前锁
     */
    private Map<String, Integer> getCurrentLockers() {
        Map<String, Integer> refs = lockers.get();
        if (refs != null) {
            return refs;
        }
        lockers.set(new HashMap<>());
        return lockers.get();
    }

    /**
     * 重入锁
     */
    public boolean reentrantlock(String key) {
        Map refs = getCurrentLockers();
        Integer refCnt = (Integer) refs.get(key);
        if (refCnt != null) {
            refs.put(key, refCnt + 1);
            return true;
        } else {
            boolean ok = this.lock(key);
            if (!ok) {
                return false;
            }
            refs.put(key, 1);
            return true;
        }
    }

    /**
     * 解除重入锁
     */
    public boolean unreentrantlock(String key) {
        Map refs = getCurrentLockers();
        Integer refCnt = (Integer) refs.get(key);
        if (refCnt == null) {
            return false;
        }
        refCnt -= 1;
        if (refCnt > 0) {
            refs.put(key, refCnt);
        } else {
            refs.remove(key);
            //注意清理，防止ThreadLock内存泄漏
            this.lockers.remove();
            this.unlock(key);
        }
        return true;
    }

    public static void main(String[] args) {
        Jedis jedis = new Jedis("127.0.0.1", 6380);
        RedisWithReentrantLock redisWithReentrantLock = new RedisWithReentrantLock(jedis);
        System.out.println(redisWithReentrantLock.reentrantlock("codehold"));
        System.out.println(redisWithReentrantLock.reentrantlock("codehold"));
        System.out.println(redisWithReentrantLock.unreentrantlock("codehold"));
        System.out.println(redisWithReentrantLock.unreentrantlock("codehold"));
        System.out.println(redisWithReentrantLock.reentrantlock("codehold"));
        System.out.println(redisWithReentrantLock.unreentrantlock("codehold"));
    }
}

package com.zcq.redisaction.transaction;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;

/**
 * redis的事务
 *
 * @author Maybeeeee
 * @date 2019-08-10 18:01
 */
public class TransactionDemo {

    public static void main(String[] args) {
        Jedis jedis = new Jedis("127.0.0.1", 6379);
        String userId = "abc";
        String key = keyFor(userId);
        jedis.setnx(key, String.valueOf(5));
        System.out.println(doubleAccount(jedis, userId));
        jedis.close();
    }

    public static int doubleAccount(Jedis jedis, String userId) {
        String key = keyFor(userId);
        //如果一次事务失败就会继续循环执行，保证操作的是最新的数据。
        while (true) {
            //如果一次事务失败,第二次循环再重新监控，尝试修改。如果这期间数据被其他线程修改。则事务失败，再进行下次循环监控
            jedis.watch(key);
            int value = Integer.parseInt(jedis.get(key));
            value *= 2; // 加倍
            Transaction tx = jedis.multi();
            tx.set(key, String.valueOf(value));
            List<Object> res = tx.exec();
            if (res.size()>0) {
                break; // 成功了
            }
        }
        return Integer.parseInt(jedis.get(key)); // 重新获取余额
    }

    public static String keyFor(String userId) {
        return String.format("account_{}", userId);
    }
}

package com.zcq.redisaction.queue;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Type;
import java.util.Set;
import java.util.UUID;

/**
 * Redis实现延时队列
 *
 * @author Maybeeeee
 * @date 2019-08-05 22:28
 */
public class RedisDelayingQueue<T> {

    //任务消息体
    static class Taskitem<T> {
        public String id;
        public T msg;
    }

    //fastjson反序列泛型类型
    private Type TaskType = new TypeReference<Taskitem<T>>() {
    }.getType();
    private Jedis jedis;
    //队列名(redis中的key)
    private String queueKey;

    public RedisDelayingQueue(Jedis jedis, String queueKey) {
        this.jedis = jedis;
        this.queueKey = queueKey;
    }

    /**
     * 投放消息
     *
     * @param msg
     */
    public void delay(T msg) {
        Taskitem taskitem = new Taskitem();
        taskitem.id = UUID.randomUUID().toString();
        taskitem.msg = msg;
        String s = JSON.toJSONString(taskitem);
        //塞入延时队列，redis的zset中，5s后可取
        jedis.zadd(queueKey, System.currentTimeMillis() + 5000, s);
    }

    /**
     * 循环获取消息
     */
    public void loop() {
        //主线程标记中断，则不循环获取了，对于这个循环线程这里主线程为consumer
        while (!Thread.interrupted()) {
            //只取一条
            Set values = jedis.zrangeByScore(queueKey, 0, System.currentTimeMillis(), 0, 1);
            if (values.isEmpty()) {
                try {
                    // 歇会继续
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    break;
                }
                continue;
            }
            String s = (String) values.iterator().next();
            //多线程环境，可能有多个线程抢到了这个消息，只有删除成功的那个才是真正抢到的。这里zrem是关键
            if (jedis.zrem(queueKey, s) > 0) {
                Taskitem taskitem = JSON.parseObject(s, TaskType);
                this.handleMsg((T) taskitem.msg);
            }
        }
    }

    /**
     * 处理消息
     *
     * @param msg
     */
    public void handleMsg(T msg) {
        System.out.println(msg);
    }

    public static void main(String[] args) {
        Jedis jedis = new Jedis("127.0.0.1", 6380);
        RedisDelayingQueue queue = new RedisDelayingQueue<>(jedis, "q-demo");

        Thread producer = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < 10; i++) {
                    //投放消息
                    queue.delay("codehole" + i);
                }
            }
        };

        Thread consumer = new Thread() {
            @Override
            public void run() {
                queue.loop();
            }
        };

        producer.start();
        consumer.start();
        try {
            producer.join();
            Thread.sleep(6000);
            consumer.interrupt();
            consumer.join();
        } catch (InterruptedException e) {
        }
    }
}

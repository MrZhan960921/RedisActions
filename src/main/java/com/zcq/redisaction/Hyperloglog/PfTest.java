package com.zcq.redisaction.Hyperloglog;

import redis.clients.jedis.Jedis;

/**
 * HyperLogLog数据误差测试(用于统计用户量，并对精确度不高的业务，节约内存空间)
 * @author Maybeeeee
 * @date 2019-08-07 23:04
 */
public class PfTest {
    public static void main(String[] args) {
        Jedis jedis=new Jedis("127.0.0.1",6380);
        for(int i=0;i<1000;i++){
            jedis.pfadd("codehole","user"+i);
            long total=jedis.pfcount("codehole");
            if(total!=i+1){
                System.out.printf("%d%d\n",total,i+1);
                break;
            }
        }
        jedis.close();
    }
}

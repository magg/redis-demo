package com.magg.repository;

import com.magg.util.RedisUtil;
import java.util.Set;
import org.springframework.stereotype.Repository;

@Repository
public class ZsetRepository
{
    private final RedisUtil redisUtil;

    private static final String ZSET_NAME = "statements.workers.zset";

    public ZsetRepository(RedisUtil redisUtil)
    {
        this.redisUtil = redisUtil;
    }

    public Boolean add(String queueName) {
        return redisUtil.zAdd(ZSET_NAME, queueName, 0.0d);
    }

    public Double incr(String queueName) {
        return redisUtil.zIncrementScore(ZSET_NAME, queueName, 1.0d);
    }

    public Double decr(String queueName) {
        return redisUtil.zIncrementScore(ZSET_NAME, queueName, -1.0d);
    }

    public Set<String> getAll() {
        return redisUtil.zRange(ZSET_NAME,0, -1);
    }

    public Set<String> getOrdered() {
        return redisUtil.zRangeByScore(ZSET_NAME, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    public Long remove(String queueName) {
        return redisUtil.zRemove(ZSET_NAME, queueName);
    }

    public Double get(String value) {
        return redisUtil.zScore(ZSET_NAME, value);
    }
}

package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisSink<T> extends RichSinkFunction<T> {
    private transient JedisPool jedisPool;
    private String redisHost;
    private int redisPort;
    private String redisPassword;

    public RedisSink(String host, int port, String password) {
        this.redisHost = host;
        this.redisPort = port;
        this.redisPassword = password;
    }

    @Override
    public void open(Configuration parameters) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);

        if (redisPassword != null && !redisPassword.isEmpty()) {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 2000, redisPassword);
        } else {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 2000);
        }
    }

    @Override
    public void invoke(T value, Context context) {
        try (Jedis jedis = jedisPool.getResource()) {
            // 实现具体的Redis写入逻辑
            if (value instanceof Tuple2) {
                Tuple2<String, Integer> tuple = (Tuple2<String, Integer>) value;
                jedis.hset("user:action:stats", tuple.f0, String.valueOf(tuple.f1));
            }
        } catch (Exception e) {
            System.err.println("Redis write error: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}

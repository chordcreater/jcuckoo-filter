package com.asdust.cuckoofilter.redis;

import org.redisson.Redisson;
import org.redisson.RedissonBitSet;
import org.redisson.api.RBitSetAsync;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.redisson.command.CommandBatchService;
import org.redisson.config.Config;

/**
 * author: chordCreater
 * date: 2022/7/31 22:00
 * desc：
 **/
public class RedisUtils extends Redisson{

    private String name;
    public RedisUtils(Config config, String name){
        super(config);
        this.name = name;
    }

    public RBitSetAsync createBitSet(CommandBatchService executorService) {
        return new RedissonBitSet(executorService, this.name);
    }
}

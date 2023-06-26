package com.asdust.cuckoofilter.redis;

/**
 * author: chordCreater
 * date: 2022/8/14 16:44
 * desc：
 **/
public class RedisConfig {

    private String address;

    private String redisBitKey;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getRedisBitKey() {
        return redisBitKey;
    }

    public void setRedisBitKey(String redisBitKey) {
        this.redisBitKey = redisBitKey;
    }
}

package com.asdust.cuckoofilter.core;

import com.alibaba.fastjson.JSON;
import com.asdust.cuckoofilter.redis.RedisConfig;
import com.asdust.cuckoofilter.redis.RedisUtils;
import com.google.common.math.DoubleMath;
import org.redisson.api.RBitSetAsync;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandBatchService;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Jianxing.Huang2
 * date: 2022/8/1 23:13
 */
public class FilterTable {
    /**
     * 日志
     */
    private static final Logger log = LoggerFactory.getLogger(FilterTable.class);
    /**
     * 最大容量
     */
    private long numBuckets;
    /**
     * 每个桶的大小为4，即可以存放4个key
     */
    private static final int BUCKET_SIZE = 4;
    /**
     * todo 负载因子,超过后，则以2倍扩容
     */
    private static final double LOAD_FACTOR = 0.955;
    /**
     * 指纹位数,最后通过计算公式得出最佳数量，即每个桶的大小，位数越高，精确度越高
     */
    private static final int BITS_PER_TAG = 16;

    private static RedisUtils redisUtils;

    private CommandAsyncExecutor commandExecutor;

    public FilterTable(long estimatedMaxNumKeys, RedisConfig redisConfig) {
        // 计算出实际的桶的个数
        this.numBuckets = getBucketsNeeded(estimatedMaxNumKeys, LOAD_FACTOR, BUCKET_SIZE);
        log.info("布谷鸟过滤器桶大小：{}", this.numBuckets);
        // 计算出需要申请的bitmap大小
        Long bitMapSize = this.numBuckets * BUCKET_SIZE * BITS_PER_TAG;
        log.info("redis的bitmap需要的bit总数：{}", bitMapSize);
        // 初始化redis
        Config config = new Config();
        config.useSingleServer().setAddress(redisConfig.getAddress());
        redisUtils = new RedisUtils(config, redisConfig.getRedisBitKey());
        commandExecutor = redisUtils.getCommandExecutor();
        log.info("redis的连接成功！");
    }

    /**
     * Calculates how many buckets are needed to hold the chosen number of keys,
     * taking the standard load factor into account.
     *
     * @param maxKeys the number of keys the filter is expected to hold before
     *                insertion failure.
     * @return The number of buckets needed
     */
    public static long getBucketsNeeded(long maxKeys, double loadFactor, int bucketSize) {
        /*
         * force a power-of-two bucket count so hash functions for bucket index
         * can hashBits%numBuckets and get randomly distributed index. See wiki
         * "Modulo Bias". Only time we can get perfectly distributed index is
         * when numBuckets is a power of 2.
         * 若要存放100个key,则需要25=100/4个桶
         */
        long bucketsNeeded = DoubleMath.roundToLong((1.0 / loadFactor) * maxKeys / bucketSize, RoundingMode.UP);
        // get next biggest power of 2 ,highestOneBit获取最高位，如输入101101，输出100000,以下设置为2的整数次幂，用于后续与运算获取元素下标
        long bitPos = Long.highestOneBit(bucketsNeeded);
        if (bucketsNeeded > bitPos) {
            bitPos = bitPos << 1;
        }
        return bitPos;
    }

    public long hashIndex(long hash) {
        // 左移tag位，只用移动后的数来获取槽索引，可以使相近的hash值key在table中更加分散
        long hashValue = hash >>> BITS_PER_TAG;
        // hash值与桶的个数减1逻辑与运算，计算出元素的索引：该计算与hashMap一样需要满足桶的个数需要为2的整数次幂方
        return hashValue & (numBuckets - 1);
    }

    public long altHashIndex(long curIndex, long tag) {
        /*
         * 0xc4ceb9fe1a85ec53L hash mixing constant from
         * MurmurHash3...interesting. Similar value used in reference
         * implementation https://github.com/efficient/cuckoofilter/
         * 可以用到异或的自反性： A ⊕ B ⊕ B = A
         */
        long altIndex = curIndex ^ (tag * 0xc4ceb9fe1a85ec53L);
        // flip bits if negative
        if (altIndex < 0) {
            altIndex = ~altIndex;
        }
        // now pull into valid range
        return hashIndex(altIndex);
    }

    public long getFingerprint(long hashVal) {
        long unusedBits = Long.SIZE - BITS_PER_TAG;
        return (hashVal << unusedBits) >>> unusedBits;
    }

    public Boolean insert(long curIndex, long tag) {
        return writeBits(curIndex, tag, true);
    }

//    private boolean writeBitsTrue(long curIndex, long tag) {
//        CommandBatchService executorService = new CommandBatchService(commandExecutor);
//        RBitSetAsync bs = redisUtils.createBitSet(executorService);
//        // 判断curIndex出是否已有值
//        log.info("writeBits：tag-bit：{}", Long.toBinaryString(tag));
//        for (int i = 0; i < BUCKET_SIZE; i++) {
//            // todo 检查与下面的设置要加同步锁才能保证原子性，检查index出i位置是否已存在tag
//            if (isExistTag(curIndex, i, tag)) {
//                log.info("writeBits：continue：{}", i);
//                continue;
//            }
//            long[] bitIndexes = getPosOfTrue(curIndex, i, tag);
//            System.out.println("writeBits：bitIndexes：" + JSON.toJSONString(bitIndexes));
//            for (long bitIndex : bitIndexes) {
//                // 将位下标对应位设置1
//                if (bitIndex != -1) {
//                    bs.setAsync(bitIndex, true);
//                    System.out.println("setAsync1");
//                }
//            }
//            // 返回修改前的值，这里应该全部返回true
//            List<Boolean> results = (List<Boolean>) executorService.execute().getResponses();
//            log.info("writeBits：results：{}", JSON.toJSONString(results));
//            return true;
////            for (Boolean val : results.subList(1, results.size() - 1)) {
////                if (!val) {
////                    System.out.println("writeBits:true");
////                    return true;
////                }
////            }
//        }
//        System.out.println("writeBits:false");
//        return false;
//    }

    public boolean delete(long curIndex, long tag) {
        return writeBits(curIndex, tag, false);
    }
    private boolean writeBits(long curIndex, long tag, Boolean bitValue) {
        CommandBatchService executorService = new CommandBatchService(commandExecutor);
        RBitSetAsync bs = redisUtils.createBitSet(executorService);
        // 判断curIndex出是否已有值
        log.info("writeBits：tag-bit：{}", Long.toBinaryString(tag));
        for (int i = 0; i < BUCKET_SIZE; i++) {
            // todo 检查与下面的设置要加同步锁才能保证原子性，检查index出i位置是否已存在tag。与redission的bloom过滤器无需加锁不同，
            //  因为它不是一个桶只存放一个元素，而是元素共用bit位，添加元素时，只要有一个bit位从0变成1，则表示添加成功，否则失败。
            //  所以利用redis本身的单进程处理实现了put的原子性，而线程安全
            if (isExistTag(curIndex, i, tag)  == bitValue) {
                log.info("writeBits：continue：{}", i);
                continue;
            }
            long[] bitIndexes = getPosOfTrue(curIndex, i, tag);
            System.out.println("writeBits：bitIndexes：" + JSON.toJSONString(bitIndexes));
            for (long bitIndex : bitIndexes) {
                // 将位下标对应位设置1或0
                if (bitIndex != -1) {
                    bs.setAsync(bitIndex, bitValue);
                }
            }
            // 返回修改前的值，若是插入值，这里应该全部返回true，否则返回false
            List<Boolean> results = (List<Boolean>) executorService.execute().getResponses();
            log.info("writeBits：results：{}", JSON.toJSONString(results));
//            for (Boolean val : results.subList(1, results.size() - 1)) {
//                if (val.equals(bitValue)) {
//                    isExistTag(curIndex, i, tag);
//                    System.out.println("writeBits:true");
//                    return bitValue;
//                }
//            }
            return true;
        }
//        System.out.println("writeBits:false");
        return false;
    }

    private boolean isExistTag(long curIndex, int posInBucket, long tag) {
        // 检查指定桶的pos处是否存在tag
        log.info("checkTag:curIndex:" + curIndex + ",posInBucket:" + posInBucket + ",tag:" + tag);
        CommandBatchService executorService = new CommandBatchService(commandExecutor);
        RBitSetAsync bs = redisUtils.createBitSet(executorService);
        long startPos = curIndex * BUCKET_SIZE * BITS_PER_TAG + (long) posInBucket * BITS_PER_TAG;
        long endPos = startPos + BITS_PER_TAG;
        for (long i = startPos; i < endPos; i++) {
            bs.getAsync(i);
        }
        List<Boolean> result = (List<Boolean>) executorService.execute().getResponses();
//        System.out.println("checkTag:results:" + JSON.toJSONString(result));
        for (int i = 0; i < BITS_PER_TAG; i++) {
            // 比如tag=15,bit表示0000 0000 0000 1111
            if (((tag & (1L << i)) == 0) == result.get(i)) {
                // 有一个bit不相同，就表示不存在，返回false
                return false;
            }
        }
//        System.out.println("checkTag:results:-----------true---------------------------");
        return true;
    }

    private long[] getPosOfTrue(long curIndex, int posInBucket, long tag) {
        long[] indexes = new long[BITS_PER_TAG];
        Arrays.fill(indexes, -1);
        // todo 这里的乘法可能造成溢出发生
        System.out.println("tag:" + tag + ",二进制：" + Long.toBinaryString(tag));
        long startPos = curIndex * BUCKET_SIZE * BITS_PER_TAG + (long) posInBucket * BITS_PER_TAG;
        for (int i = 0; i < BITS_PER_TAG; i++) {
            // 比如tag=15,bit表示0000 0000 0000 1111
            if ((tag & (1L << i)) != 0) {
                indexes[i] = startPos + i;
            }
        }
        return indexes;
    }

    public long randSelectTag(long curIndex, long tag) {
        // 随机从槽位中选取一个元素  注：有的实现中用了一个longBitSet的容器数组来做了一个优化，减少空间的浪费，可以对更大数据量的数据进行过滤，具体了参考项目:
        //https://github.com/MGunlogson/CuckooFilter4J
        int randomBucketPosition = ThreadLocalRandom.current().nextInt(BUCKET_SIZE);
        long startPos = curIndex * BUCKET_SIZE * BITS_PER_TAG + (long) randomBucketPosition * BITS_PER_TAG;
        long endPos = startPos + BITS_PER_TAG;
        long oldTag = 0;
        CommandBatchService executorService = new CommandBatchService(commandExecutor);
        RBitSetAsync bs = redisUtils.createBitSet(executorService);
        for (long i = startPos; i < endPos; i++) {
            bs.getAsync(startPos);
        }
        List<Boolean> result = (List<Boolean>) executorService.execute().getResponses();
        for (int i = 0; i < BITS_PER_TAG; i++) {
            // 比如tag=15,bit表示0000 0000 0000 1111
            long curBit = tag & (1L << i);
            Boolean oldCurBitBool = result.get(i);
            if ((curBit == 1) && !oldCurBitBool) {
                bs.setAsync(startPos + i, true);
            } else if ((curBit == 0) && oldCurBitBool) {
                bs.setAsync(startPos + i, false);
            }
            int oldCurBit = oldCurBitBool ? 1 : 0;
            oldTag = oldTag | (oldCurBit << i);
        }
        return oldTag;
    }

    public boolean contain(long curIndex, long altIndex, long tag) {
        // 判断curIndex出是否已有值
        for (int i = 0; i < BUCKET_SIZE; i++) {
            // 检查index在桶的i位置是否已存在值0，若存在表示该处没有值
            if (isExistTag(curIndex, i, tag) || isExistTag(altIndex, i, tag)) {
                return true;
            }
        }
        return false;
    }


}

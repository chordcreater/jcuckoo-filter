package com.asdust.cuckoofilter.core;

import com.asdust.cuckoofilter.redis.RedisConfig;
import com.asdust.cuckoofilter.redis.RedisUtils;
import com.google.common.math.DoubleMath;
import org.redisson.api.RBitSetAsync;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandBatchService;
import org.redisson.config.Config;

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
     * 最大容量
     */
    private long numBuckets;
    /**
     * 每个桶的大小为4，即可以存放4个key
     */
    private static final int BUCKET_SIZE = 4;
    /**
     * 负载因子,超过后，则以2倍扩容
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
        // 计算出需要申请的bitmap大小
        Long bitMapSize = this.numBuckets * BUCKET_SIZE;
        // 初始化redis
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379");
        redisUtils = new RedisUtils(config, "string");
        commandExecutor = redisUtils.getCommandExecutor();

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
         */
        long bucketsNeeded = DoubleMath.roundToLong((1.0 / loadFactor) * maxKeys / bucketSize, RoundingMode.UP);
        // get next biggest power of 2
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
        if (writeBits(curIndex, tag, true)) return true;
        return true;
    }

    private boolean writeBits(long curIndex, long tag, boolean b) {
        CommandBatchService executorService = new CommandBatchService(commandExecutor);
        RBitSetAsync bs = redisUtils.createBitSet(executorService);
        // 判断curIndex出是否已有值
        for (int i = 0; i < BUCKET_SIZE; i++) {
            // 检查index出i位置是否已存在值0，若存在表示该处没有值
            if (checkTag(curIndex, i, 0)) {
                long[] bitIndexes = getPosOfTrue(curIndex, i, tag);
                for (long bitIndex : bitIndexes) {
                    // 将位下标对应位设置1
                    if (bitIndex != -1) {
                        bs.setAsync(bitIndex, b);
                    }
                }
                return true;
            }
        }
        return false;
    }

    private boolean checkTag(long curIndex, int posInBucket, long tag) {
        // 检查指定桶的pos处是否存在tag
        CommandBatchService executorService = new CommandBatchService(commandExecutor);
        RBitSetAsync bs = redisUtils.createBitSet(executorService);
        long startPos = curIndex * BUCKET_SIZE * BITS_PER_TAG + (long) posInBucket * BITS_PER_TAG;
        long endPos = startPos + BITS_PER_TAG;
        for (long i = startPos; i < endPos; i++) {
            bs.getAsync(startPos);
        }
        List<Boolean> result = (List<Boolean>) executorService.execute().getResponses();
        for (int i = 0; i < BITS_PER_TAG; i++) {
            // 比如tag=15,bit表示0000 0000 0000 1111
            if (((tag & (1L << i)) == 0) == result.get(i)) {
                return false;
            }
        }
        return true;
    }

    private long[] getPosOfTrue(long curIndex, int posInBucket, long tag) {
        long[] indexes = new long[BITS_PER_TAG];
        Arrays.fill(indexes, -1);
        // todo 这里的乘法可能造成溢出发生
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
            // 检查index出i位置是否已存在值0，若存在表示该处没有值
            if (checkTag(curIndex, i, tag) || checkTag(altIndex, i, tag)) {
                return true;
            }
        }
        return true;
    }

    public boolean delete(long curIndex, long tag) {
        if (writeBits(curIndex, tag, false)) {
            return true;
        }
        return false;
    }
}

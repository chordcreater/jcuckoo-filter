package com.asdust.cuckoofilter;

import com.asdust.cuckoofilter.redis.RedisConfig;
import com.asdust.cuckoofilter.redis.RedisUtils;
import com.google.common.math.DoubleMath;
import org.redisson.api.RBitSetAsync;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandBatchService;
import org.redisson.config.Config;

import java.math.RoundingMode;

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
     * 指纹字节数,最后通过计算公式得出最佳数量
     */
    private final int TAG_BITS = 4;

    /**
     * 每个桶的大小为4，即可以存放4个key
     */
    private static final int BUCKET_SIZE = 4;
    /**
     * 负载因子,超过后，则以2倍扩容
     */
    private static final double LOAD_FACTOR = 0.955;

    private static RedisUtils redisUtils;

    private CommandAsyncExecutor commandExecutor;

    public FilterTable(long estimatedMaxNumKeys, RedisConfig redisConfig) {
        // 计算出实际的桶的个数
        this.numBuckets = getBucketsNeeded(estimatedMaxNumKeys, LOAD_FACTOR, BUCKET_SIZE);
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379");
        redisUtils = new RedisUtils(config, "string");
        commandExecutor = redisUtils.getCommandExecutor();
//        RBloomFilter<String> bloomFilter = redisUtils.getBloomFilter("bloom-filter");
//        // 初始化布隆过滤器
//        bloomFilter.tryInit(200, 0.01);
    }

    private void tryInit(long numBuckets) {
//        this.size = this.optimalNumOfBits(numBuckets);
//        CommandBatchService executorService = new CommandBatchService(this.commandExecutor);
//        executorService.evalReadAsync(this.configName, this.codec, RedisCommands.EVAL_VOID, "local size = redis.call('hget', KEYS[1], 'size');local hashIterations = redis.call('hget', KEYS[1], 'hashIterations');assert(size == false and hashIterations == false, 'Bloom filter config has been changed')", Arrays.asList(this.configName), new Object[]{this.size, this.hashIterations});
//        executorService.writeAsync(this.configName, StringCodec.INSTANCE, new RedisCommand("HMSET", new VoidReplayConvertor()), new Object[]{this.configName, "size", this.size, "hashIterations", this.hashIterations, "expectedInsertions", expectedInsertions, "falseProbability", BigDecimal.valueOf(falseProbability).toPlainString()});
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
        // 左移tag位，只用移动后的数来获取槽索引，可以是相近的hash值key在table中更加分散
        long hashValue = hash >>> TAG_BITS;
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
        long unusedBits = Long.SIZE - TAG_BITS;
        return (hashVal << unusedBits) >>> unusedBits;
    }

    private long[] hash(long tag) {
        return null;
    }

    public Boolean insert(long curIndex, long tag) {
        //
        long startPos = curIndex * BUCKET_SIZE;
        long endPos = curIndex * BUCKET_SIZE;
        long[] indexes = hash(tag);
        CommandBatchService executorService = new CommandBatchService(commandExecutor);
        RBitSetAsync bs = redisUtils.createBitSet(executorService);
        // 遍历槽位的 4 个元素，如果为空则插入
        for (int i = 0; i < indexes.length; i++) {
            // 将位下标对应位设置1
            bs.setAsync(indexes[i]);
        }

        return true;
    }

    public long randSelectTag(long index, long tag){
        // 随机从槽位中选取一个元素

        // 插入新值

        return 0L;
    }

    public boolean contain(long curIndex, long altIndex, long tag) {

        return true;
    }

    public boolean delete(long curIndex, long tag) {

        return true;
    }
}

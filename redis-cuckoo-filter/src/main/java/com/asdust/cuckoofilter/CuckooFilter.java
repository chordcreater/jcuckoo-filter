package com.asdust.cuckoofilter;

import com.asdust.cuckoofilter.core.FilterTable;
import com.asdust.cuckoofilter.core.ItemPos;
import com.asdust.cuckoofilter.redis.RedisConfig;
import com.asdust.cuckoofilter.utils.HashUtils;
import com.google.common.math.DoubleMath;

import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: chordCreater
 * @date: 2022/7/31 17:35
 * @desc:
 */
public class CuckooFilter<T> {
    private FilterTable table;

    private static final int MAX_TRY_CUCKOO_COUNT = 100;

    private final AtomicLong numItems = new AtomicLong(0);

    /**
     * @param estimatedMaxNumKeys 期望存放的最大key个数
     */
    public CuckooFilter(Long estimatedMaxNumKeys, RedisConfig redisConfig) {
        table = new FilterTable(estimatedMaxNumKeys, redisConfig);
    }
    /**
     * Calculates how many bits are needed to reach a given false positive rate.
     *
     * @param fpProb the false positive probability.
     * @return the length of the tag needed (in bits) to reach the false
     * positive rate.
     * 用来计算最佳的指纹位数，本工具没有使用，使用了固定值16
     */
    public static int getBitsPerItemForFpRate(double fpProb, double loadFactor) {
        /*
         * equation from Cuckoo Filter: Practically Better Than Bloom Bin Fan,
         * David G. Andersen, Michael Kaminsky , Michael D. Mitzenmacher
         */
        return DoubleMath.roundToInt(DoubleMath.log2((1 / fpProb) + 3) / loadFactor, RoundingMode.UP);
    }

    private ItemPos generateIndexTagHash(T item) {
        String itemStr;
        if (item instanceof String) {
            itemStr = (String) item;
        } else if (item instanceof Long) {
            itemStr = ((Long) item).toString();
        } else if (item instanceof Integer) {
            itemStr = ((Integer) item).toString();
        } else if (item instanceof Byte[]) {
            itemStr = new String((byte[]) item, StandardCharsets.UTF_8);
        } else {
            throw new IllegalArgumentException("传入参数非法");
        }
        long hash = HashUtils.hash(itemStr);
        ItemPos itemPos = new ItemPos();
        long curIndex = table.hashIndex(hash);
        long tag = table.getFingerprint(hash);
        itemPos.setCurIndex(curIndex);
        itemPos.setTag(tag);
        return itemPos;
    }


    /**
     * 写入item
     *
     * @param item
     * @return true or false
     */
    public boolean put(T item) {
        long curIndex, altIndex, tag;
        // 计算出索引和指纹以及另一个索引
        ItemPos itemPos = generateIndexTagHash(item);
        curIndex = itemPos.getCurIndex();
        tag = itemPos.getTag();
        altIndex = table.altHashIndex(curIndex, tag);
        System.out.println("put:curIndex:"+curIndex+",altIndex:"+altIndex+",:tag:"+tag);
        if (table.insert(curIndex, tag) || table.insert(altIndex, tag)) {
            numItems.incrementAndGet();
            System.out.println("incrementAndGet");
            return true;
        }
        System.out.println("put:MAX_TRY_CUCKOO_COUNT");
        //全部已满，则从槽1或槽2中随机剔除一个值的位置插入，然后将新的值插入到新的槽中，如此循环直到插入成功，或者达到最大值100
        for (int count = 1; count < MAX_TRY_CUCKOO_COUNT; count++) {
            // 随机获取一个槽的tag
            long index = randSelectSlot(curIndex, altIndex);
            long oldTag = table.randSelectTag(index, tag);
            if (table.insert(curIndex, oldTag)) {
                return true;
            }
            curIndex = index;
            altIndex = table.altHashIndex(curIndex, oldTag);
        }
        //todo 若到达最大尝试次数，则使用victim(牺牲者)将被剔除这缓存起来，仍然返回true
        numItems.incrementAndGet();
        return true;
    }

    private long randSelectSlot(long curIndex, long altIndex) {
        int randNum = ThreadLocalRandom.current().nextInt(2);
        return randNum == 0 ? curIndex : altIndex;
    }

    /**
     * 判断是否存在key
     *
     * @return true or false
     */
    public boolean contain(T item) {
        // 计算出两个索引的位置
        long curIndex, tag, altIndex;
        // 计算出索引和指纹以及另一个索引
        ItemPos itemPos = generateIndexTagHash(item);
        curIndex = itemPos.getCurIndex();
        tag = itemPos.getTag();
        altIndex = table.altHashIndex(curIndex, tag);
        // 查询是否存在table中
        System.out.println("contain:curIndex:"+curIndex+",altIndex:"+altIndex+",:tag:"+tag);
        return table.contain(curIndex, altIndex, tag);
    }

    /**
     * 删除指定元素
     *
     * @param item
     * @return true or false
     */
    public boolean delete(T item) {
        // 计算出两个索引的位置
        long curIndex, tag, altIndex;
        // 计算出索引和指纹以及另一个索引
        ItemPos itemPos = generateIndexTagHash(item);
        curIndex = itemPos.getCurIndex();
        tag = itemPos.getTag();
        altIndex = table.altHashIndex(curIndex, tag);
        // 存在则删除
        if (table.delete(curIndex, tag) || table.delete(altIndex, tag)) {
            numItems.decrementAndGet();
            System.out.println("decrementAndGet");
            return true;
        }
        //todo 否则检查是否为victim，若是则设置victim为false
        return false;
    }


    /**
     * 获取存放的元素数量
     *
     * @return @return number of items in filter
     */
    public long size() {
        // can return more than maxKeys if running above design limit!
        return numItems.get();
    }
}

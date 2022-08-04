package com.asdust.cuckoofilter;

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
    private final int tagBits = 4;
    public FilterTable(long estimatedMaxNumKeys){

        this.numBuckets = numBuckets;
    }

    public long hashIndex(long hash){
        // 左移tag位，只用移动后的数来获取槽索引，可以是相近的hash值key在table中更加分散
        long hashValue =hash >>> tagBits;
        return hashValue & (numBuckets -1);
    }

    public Long altHashIndex(long bucketIndex,long tag){
        /*
         * 0xc4ceb9fe1a85ec53L hash mixing constant from
         * MurmurHash3...interesting. Similar value used in reference
         * implementation https://github.com/efficient/cuckoofilter/
         * 可以用到异或的自反性： A ⊕ B ⊕ B = A
         */
        long altIndex = bucketIndex ^ (tag * 0xc4ceb9fe1a85ec53L);
        // flip bits if negative
        if (altIndex < 0){
            altIndex = ~altIndex;
        }
        // now pull into valid range
        return hashIndex(altIndex);
    }
    public Long getFingerprint(long hashVal){
        long unusedBits = Long.SIZE - tagBits;
        return (hashVal << unusedBits) >>> unusedBits;
    }

}

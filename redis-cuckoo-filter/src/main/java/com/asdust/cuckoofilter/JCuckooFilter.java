package com.asdust.cuckoofilter;

import com.asdust.cuckoofilter.utils.Utils;

/**
 * @author: chordCreater
 * @date: 2022/7/31 17:35
 * @desc:
 */
public class JCuckooFilter<T> {
    private FilterTable table;
    // 每个桶的大小为4，即可以存放4个key
    static final int BUCKET_SIZE = 4;
    // 负载因子,超过后，则以2倍扩容
    private static final double LOAD_FACTOR = 0.955;
    /**
     * TODO methods
     * @param max_num_keys 期望存放的最大key个数
     */
    public JCuckooFilter(Long max_num_keys){
        // 计算出实际的桶的个数
        long numBuckets = Utils.getBucketsNeeded(max_num_keys, LOAD_FACTOR, BUCKET_SIZE);
        table = new FilterTable(numBuckets);

    }

    /**
     * TODO methods
     * @param item
     * @return true or false
     */
    public boolean put(T item){

        return true;
    }
    /**
     * TODO methods
     * @return true or false
     */
    public boolean contain(){
        return true;
    }
    /** 
     * TODO methods
     * @param item
     * @return true or false
     */
    public boolean delete(T item){
        return true;
    }


    /**
     * TODO methods
     * @return @return number of items in filter
     */
    public long size() {
        // can return more than maxKeys if running above design limit!
        return 0L;
    }
}

package com.asdust.cuckoofilter;

import com.asdust.cuckoofilter.utils.Utils;

/**
 * @author: chordCreater
 * @date: 2022/7/31 17:35
 * @desc:
 */
public class CuckooFilter<T> {
    private FilterTable table;
    /**
     * 每个桶的大小为4，即可以存放4个key
    */
    private static final int BUCKET_SIZE = 4;
    /**
     * 负载因子,超过后，则以2倍扩容
     */
    private static final double LOAD_FACTOR = 0.955;


    private static final int MAX_TRY_CUCKOO_COUNT = 500;
    /**
     * TODO methods
     * @param max_num_keys 期望存放的最大key个数
     */
    public CuckooFilter(Long max_num_keys){
        // 计算出实际的桶的个数
        long numBuckets = Utils.getBucketsNeeded(max_num_keys, LOAD_FACTOR, BUCKET_SIZE);
        table = new FilterTable(numBuckets);

    }

    /**
     * 写入item
     * @param item
     * @return true or false
     */
    public boolean put(T item){
        // 计算出索引和指纹以及另一个索引

        // 循环写入table，若写入成功则退出，写入失败，则弹出占位值，作为新值插入，如此循环直到插入成功，或者达到最大值500

        //若到达最大尝试次数，则使用victim(牺牲者)将被剔除这缓存起来，任然返回true

        return true;
    }
    /**
     * 判断是否存在key
     * @return true or false
     */
    public boolean contain(){
        // 计算出两个索引的位置

        // 查询是否存在table中

        return true;
    }
    /** 
     * TODO methods
     * @param item
     * @return true or false
     */
    public boolean delete(T item){
        // 计算出两个索引的位置

        // 存在则删除

        //否则检查是否为victim，若是则设置victim为false

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

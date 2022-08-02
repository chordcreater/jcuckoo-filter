package com.asdust.cuckoofilter;

/**
 * author: chordCreater
 * date: 2022/8/1 23:13
 * desc：
 **/
public class FilterTable {
    // 最大容量
    private long numBuckets;
    // 指纹字节数
    public FilterTable(long numBuckets){
        this.numBuckets = numBuckets;
    }
}

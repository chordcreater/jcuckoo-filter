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
    public FilterTable(long numBuckets){
        this.numBuckets = numBuckets;
    }


}

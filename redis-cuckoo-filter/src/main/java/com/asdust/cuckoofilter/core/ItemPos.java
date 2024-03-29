package com.asdust.cuckoofilter.core;

/**
 * @author: Jianxing.Huang
 * @date: 2022/8/4 14:07
 * @desc:
 */
public class ItemPos {
    /**
     * 位置下标
     */
    private long curIndex;

    /**
     * 指纹
     */
    private long tag;

    public long getCurIndex() {
        return curIndex;
    }

    public void setCurIndex(long curIndex) {
        this.curIndex = curIndex;
    }

    public long getTag() {
        return tag;
    }

    public void setTag(long tag) {
        this.tag = tag;
    }

}

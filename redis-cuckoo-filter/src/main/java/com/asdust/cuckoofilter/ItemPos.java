package com.asdust.cuckoofilter;

/**
 * @author: Jianxing.Huang
 * @date: 2022/8/4 14:07
 * @desc:
 */
public class ItemPos {
    private long curIndex;

    private long altIndex;
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

    public long getAltIndex() {
        return altIndex;
    }

    public void setAltIndex(long altIndex) {
        this.altIndex = altIndex;
    }
}

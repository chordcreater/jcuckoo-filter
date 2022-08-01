package com.asdust.cuckoofilter;

/**
 * @author: chordCreater
 * @date: 2022/7/31 17:35
 * @desc:
 */
public class JCuckooFilter<T> {
    private FilterTable table;

    public JCuckooFilter(){
        table = new FilterTable();
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

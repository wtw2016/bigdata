package com.wtw.hadoop.mr.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
* 按照年份进行分组比较
* */
public class YearGroupComparator extends WritableComparator {
    protected YearGroupComparator() {
        super(ComboKey.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        ComboKey k1 = (ComboKey)a ;
        ComboKey k2 = (ComboKey)b ;
        return k1.getYear() - k2.getYear() ;
    }
}

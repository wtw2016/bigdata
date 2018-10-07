package com.wtw.hadoop.mr.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
* key排序对比器
* */
public class ComboKeySortComparator extends WritableComparator {

    protected ComboKeySortComparator() {
        super(ComboKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComboKey k1 = (ComboKey)a;
        ComboKey k2 = (ComboKey)b;
        return k1.compareTo(k2);
    }
}

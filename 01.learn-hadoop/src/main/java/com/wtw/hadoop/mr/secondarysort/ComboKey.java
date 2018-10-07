package com.wtw.hadoop.mr.secondarysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
* 自定义key来实现二次排序
* */
public class ComboKey implements WritableComparable<ComboKey> {
    private int year;
    private int temp;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    /*
    * 对key进行排序
    * */
    @Override
    public int compareTo(ComboKey o) {
        int year = o.getYear();
        int temp = o.getTemp();

        if (this.year == year) { // 年份升序
            return -(this.temp - temp); // 温度降序
        } else {
            return this.year - year;
        }
    }

    /*
     *串行化
     * */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(temp);
    }

    /*
    * 反序列化成一个ComboKey对象
    * */
    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
        temp = in.readInt();
    }

    @Override
    public String toString() {
        return "(" + this.year + ", " + this.temp + ")";
    }
}

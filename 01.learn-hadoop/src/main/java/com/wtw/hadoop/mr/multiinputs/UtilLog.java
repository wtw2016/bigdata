package com.wtw.hadoop.mr.multiinputs;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class UtilLog {
    public static String getInfo(Object obj, String msg) {
        return getHostName() + ":"
                +getPID() + ":"
                +getTID() + ":"
                +getObjInfo(obj) + ":"
                +msg;
    }

    public static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static int getPID(){
        String info = ManagementFactory.getRuntimeMXBean().getName();
        return Integer.parseInt(info.substring(0, info.indexOf('@')));
    }

    public static String getTID(){
        return Thread.currentThread().getName();
    }

    public static String getObjInfo(Object obj) {
        String name = obj.getClass().getSimpleName();
        return name + "@" + obj.hashCode();
    }
}

package com.xy.topology.utils;

import java.util.Random;

/**
 * @author zanhonglei
 * @description：工具类
 * @date 2019 2019/7/18 19:42
 * @version:
 * @modified By：
 */
public class Utils {


    public static void sleep(int m) {
        try {
            Thread.sleep(m);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * length用户要求产生字符串的长度
     * @param length
     * @return
     */
    public static String getRandomString(int length){
        String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random=new Random();
        StringBuffer sb=new StringBuffer();
        for(int i=0;i<length;i++){
            int number=random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }
}

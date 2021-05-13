package com.ss.util;

import cn.hutool.core.date.DateTime;
import cn.hutool.crypto.SecureUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;

/**
 * Created by zhaozh on 2021/05/12.
 */
public class Utils {

    public static String generateRowKey(String id)
    {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(Bytes.toBytes(id));
            System.out.println("md5 digest bytes length: " + digest.length);    // returns 16
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return "";
    }

    public static String generateRowKey2(String id) {
        long timeStamp = System.currentTimeMillis();
        long reverseTimeStamp = Long.MAX_VALUE - timeStamp;

        System.out.println("timeStamp=" + timeStamp);
        System.out.println("reverseTimeStamp=" + reverseTimeStamp);

        String idMD5 = SecureUtil.md5(id);
        System.out.println("idMD5=" + idMD5);

        return idMD5.substring(0, 16) + id.toLowerCase() + reverseTimeStamp;
    }
}

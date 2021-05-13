package com.ss.util;

import org.junit.Test;

/**
 * Created by zhaozh on 2021/05/12.
 */
public class UtilsTest {
    @Test
    public void generateRowKey() {
        Utils.generateRowKey("H01AYGTEST0000003");
    }

    @Test
    public void generateRowKey2() {
        System.out.println(Utils.generateRowKey2("H01AYGTEST0000003"));
    }
}

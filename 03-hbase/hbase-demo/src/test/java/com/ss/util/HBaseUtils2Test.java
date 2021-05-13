package com.ss.util;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by zhaozh on 2021/05/12.
 */
public class HBaseUtils2Test {

    private static final String TABLE_NAME = "gb17691_message";
    private static final String ORIGNAL = "o";
    private static final String DESERIALIZE = "d";

    @Test
    public void createTable() {
        // 新建表
        List<String> columnFamilies = Arrays.asList(DESERIALIZE, ORIGNAL);
        boolean table = HBaseUtils.createTable(TABLE_NAME, columnFamilies);
        System.out.println("表创建结果:" + table);
    }

    @Test
    public void insertData() {
        String vin = "h01aygtest0000003";
        List<Pair<String, String>> pairs1 = Arrays.asList(new Pair<>("vin", vin),
                new Pair<>("msgid", "1"),
                new Pair<>("data", "23230148303141594754455354303030303030330001001C15050C10172C0001383938363036313830303030333938393138383013"));

        HBaseUtils.putRow(TABLE_NAME, Utils.generateRowKey2(vin), ORIGNAL, pairs1);

        List<Pair<String, String>> pairs2 = Arrays.asList(new Pair<>("vin", vin),
                new Pair<>("msgid", "2"),
                new Pair<>("data", "23230248303141594754455354303030303030330001005E15050C10203500010215050C10203514001487696D60117007D071484B33680000FAFE900F007D0008F0D180016E36000000271014525652565256525652565256525652565256525614535653565356535653565356535653565356535630"));

        HBaseUtils.putRow(TABLE_NAME, Utils.generateRowKey2(vin), ORIGNAL, pairs2);

        List<Pair<String, String>> pairs3 = Arrays.asList(new Pair<>("vin", vin),
                new Pair<>("msgid", "3"),
                new Pair<>("data", "23230348303141594754455354303030303030330001005E15050C10212B00010215050C10212B14001487696D60117007D071484B33680000FAFE900F007D0008F0D180016E36000000271014525652565256525652565256525652565256525614535653565356535653565356535653565356535631"));

        HBaseUtils.putRow(TABLE_NAME, Utils.generateRowKey2(vin), ORIGNAL, pairs3);

        List<Pair<String, String>> pairs4 = Arrays.asList(new Pair<>("vin", vin),
                new Pair<>("msgid", "4"),
                new Pair<>("data", "23230448303141594754455354303030303030330001000815050C102207000116"));

        HBaseUtils.putRow(TABLE_NAME, Utils.generateRowKey2(vin), ORIGNAL, pairs4);
    }


    @Test
    public void getRow() {
        Result result = HBaseUtils.getRow(TABLE_NAME, "bfda83ae54e8d551h01aygtest00000039223370416045374640");
        if (result != null) {
            System.out.println(Bytes
                    .toString(result.getValue(Bytes.toBytes(ORIGNAL), Bytes.toBytes("vin"))));
        }

    }

    @Test
    public void getCell() {
        String cell = HBaseUtils.getCell(TABLE_NAME, "bfda83ae54e8d551h01aygtest00000039223370416045374640", ORIGNAL, "msgid");
        System.out.println("cell age :" + cell);
    }

    @Test
    public void getScanner() {
        ResultScanner scanner = HBaseUtils.getScanner(TABLE_NAME);
        if (scanner != null) {
            scanner.forEach(result -> System.out.println(Bytes.toString(result.getRow()) + "->" + Bytes
                    .toString(result.getValue(Bytes.toBytes(ORIGNAL), Bytes.toBytes("vin")))));
            scanner.close();
        }
    }


    @Test
    public void getScannerWithFilter() {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter nameFilter = new SingleColumnValueFilter(Bytes.toBytes(ORIGNAL),
                Bytes.toBytes("vin"), CompareOperator.EQUAL, Bytes.toBytes("h01aygtest0000003"));
        filterList.addFilter(nameFilter);
        ResultScanner scanner = HBaseUtils.getScanner(TABLE_NAME, filterList);
        if (scanner != null) {
            scanner.forEach(result -> System.out.println(Bytes.toString(result.getRow()) + "->" + Bytes
                    .toString(result.getValue(Bytes.toBytes(ORIGNAL), Bytes.toBytes("vin")))));
            scanner.close();
        }
    }

    @Test
    public void getScannerWithFilter2() {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter nameFilter = new SingleColumnValueFilter(Bytes.toBytes(ORIGNAL),
                Bytes.toBytes("vin"), CompareOperator.EQUAL, Bytes.toBytes("h01aygtest0000003"));
        filterList.addFilter(nameFilter);

        //左闭，右开
        String startRowKey = "bfda83ae54e8d551h01aygtest00000039223370416045374628";
        String endRowKey = "bfda83ae54e8d551h01aygtest00000039223370416045374636";

        ResultScanner scanner = HBaseUtils.getScanner(TABLE_NAME, startRowKey, endRowKey, filterList);
        if (scanner != null) {
            scanner.forEach(result -> System.out.println(Bytes.toString(result.getRow()) + "->" + Bytes
                    .toString(result.getValue(Bytes.toBytes(ORIGNAL), Bytes.toBytes("vin"))) + "->" + Bytes
                    .toString(result.getValue(Bytes.toBytes(ORIGNAL), Bytes.toBytes("msgid"))) + "->" + Bytes
                    .toString(result.getValue(Bytes.toBytes(ORIGNAL), Bytes.toBytes("data")))));
            scanner.close();
        }
    }

    @Test
    public void deleteColumn() {
        boolean b = HBaseUtils.deleteColumn(TABLE_NAME, "rowKey2", ORIGNAL, "age");
        System.out.println("删除结果: " + b);
    }

    @Test
    public void deleteRow() {
        boolean b = HBaseUtils.deleteRow(TABLE_NAME, "rowKey2");
        System.out.println("删除结果: " + b);
    }

    @Test
    public void deleteTable() {
        boolean b = HBaseUtils.deleteTable(TABLE_NAME);
        System.out.println("删除结果: " + b);
    }
}

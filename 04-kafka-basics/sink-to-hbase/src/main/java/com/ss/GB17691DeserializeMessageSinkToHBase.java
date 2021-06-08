package com.ss;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ss.hbase.HBaseUtils;
import com.ss.hbase.Utils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Kafka消费者——同步提交
 */
public class GB17691DeserializeMessageSinkToHBase {
    private static final String TABLE_NAME = "gb17691_track";
    private static final String DESERIALIZE = "t";

    public static void main(String[] args) {
        String topic = "gb17691_deserialize_message";
        String group = "group1";
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop001:9092");
        props.put("group.id", group);
        props.put("enable.auto.commit", false);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                for (ConsumerRecord<String, String> record : records) {
                    //System.out.println(record);
                    System.out.printf("topic = %s,partition = %d, key = %s, value = %s, offset = %d,\n",
                            record.topic(), record.partition(), record.key(), record.value(), record.offset());

                    //String vin = "h01aygtest0000003";
                    /*byte msgid = Bytes.toBytes(record.value())[2];
                    List<Pair<byte[], byte[]>> pairs1 = Arrays.asList(new Pair<>(Bytes.toBytes("vin"), Bytes.toBytes(record.key())),
                            new Pair<>(Bytes.toBytes("msgid"), Bytes.toBytes(msgid)),
                            new Pair<>(Bytes.toBytes("data"), Bytes.toBytes(record.value())));

                    HBaseUtils.putRow2(TABLE_NAME, Utils.generateRowKey2(record.key()), ORIGNAL, pairs1);
                     */

                    JSONObject jsonObject = JSON.parseObject(record.value());

                    if (jsonObject.containsKey("VIN") && jsonObject.containsKey("MsgId"))
                    {
                        String vin = jsonObject.getString("VIN");
                        String msgId = jsonObject.getString("MsgId");
                        //System.out.printf(vin);
                        if (msgId.equals("2"))
                        {
                            if (jsonObject.containsKey("Bodies")) {
                                JSONObject bodies = JSON.parseObject(jsonObject.getString("Bodies"));
                                if (bodies.containsKey("ValueList")){
                                    JSONArray valueList = JSON.parseArray(bodies.getString("ValueList"));
                                    for (Object obj: valueList) {
                                        JSONObject value = (JSONObject) obj;
                                        if (value.containsKey("TypeCode"))
                                        {
                                            if (value.getInteger("TypeCode") == 2)
                                            {
                                                DateTime dateTime = new DateTime(value.getString("GpsTime"), DatePattern.NORM_DATETIME_FORMAT);
                                                long ts = dateTime.toTimestamp().getTime();
                                                List<Pair<byte[], byte[]>> pairs1 = Arrays.asList(new Pair<>(Bytes.toBytes("vin"), Bytes.toBytes(vin)),
                                                        new Pair<>(Bytes.toBytes("typecode"), Bytes.toBytes(vin)),
                                                        new Pair<>(Bytes.toBytes("gps_time"), Bytes.toBytes(ts)),
                                                        new Pair<>(Bytes.toBytes("is_supplement"), Bytes.toBytes("0")),
                                                        new Pair<>(Bytes.toBytes("speed"), Bytes.toBytes(value.getDouble("Speed").toString())),
                                                        new Pair<>(Bytes.toBytes("pressure"), Bytes.toBytes(value.getDouble("Pressure").toString())),
                                                        new Pair<>(Bytes.toBytes("net_output_torque"), Bytes.toBytes(value.getShort("NetOutputTorque").toString())),
                                                        new Pair<>(Bytes.toBytes("friction_torque"), Bytes.toBytes(value.getShort("FrictionTorque").toString())),
                                                        new Pair<>(Bytes.toBytes("rpm"), Bytes.toBytes(value.getDouble("RPM").toString())),
                                                        new Pair<>(Bytes.toBytes("fuel_flow"), Bytes.toBytes(value.getDouble("FuelFlow").toString())),
                                                        new Pair<>(Bytes.toBytes("scr_up_nox"), Bytes.toBytes(value.getDouble("SCRUpNOx").toString())),
                                                        new Pair<>(Bytes.toBytes("scr_down_nox"), Bytes.toBytes(value.getDouble("SCRDownNOx").toString())),
                                                        new Pair<>(Bytes.toBytes("reagent_allowance"), Bytes.toBytes(value.getDouble("ReagentAllowance").toString())),
                                                        new Pair<>(Bytes.toBytes("air_inflow"), Bytes.toBytes(value.getDouble("AirInflow").toString())),
                                                        new Pair<>(Bytes.toBytes("scr_inlet_temp"), Bytes.toBytes(value.getDouble("SCRInletTemp").toString())),
                                                        new Pair<>(Bytes.toBytes("scr_outlet_temp"), Bytes.toBytes(value.getDouble("SCROutletTemp").toString())),
                                                        new Pair<>(Bytes.toBytes("dpf"), Bytes.toBytes(value.getDouble("DPF").toString())),
                                                        new Pair<>(Bytes.toBytes("coolant_temp"), Bytes.toBytes(value.getShort("CoolantTemp").toString())),
                                                        new Pair<>(Bytes.toBytes("fuel_tank_level"), Bytes.toBytes(value.getDouble("FuelTankLevel").toString())),
                                                        new Pair<>(Bytes.toBytes("locate_statu"), Bytes.toBytes(value.getDouble("LocateStatu").toString())),
                                                        new Pair<>(Bytes.toBytes("lng"), Bytes.toBytes(value.getDouble("Lng").toString())),
                                                        new Pair<>(Bytes.toBytes("lat"), Bytes.toBytes(value.getDouble("Lat").toString())),
                                                        new Pair<>(Bytes.toBytes("mileage"), Bytes.toBytes(value.getDouble("Mileage").toString())));

                                                //String rowKey = Utils.generateRowKey3(vin, ts);
                                                String rowKey = Utils.generateRowKey2(vin);
                                                HBaseUtils.putRow2(TABLE_NAME, rowKey, DESERIALIZE, pairs1);
                                                System.out.printf(rowKey);
                                            }
                                            else if (value.getInteger("TypeCode") == 1)
                                            {
                                                DateTime dateTime = new DateTime(value.getString("GpsTime"), DatePattern.NORM_DATETIME_FORMAT);
                                                long ts = dateTime.toTimestamp().getTime();
                                                List<Pair<byte[], byte[]>> pairs1 = Arrays.asList(new Pair<>(Bytes.toBytes("vin"), Bytes.toBytes(vin)),
                                                        new Pair<>(Bytes.toBytes("gps_time"), Bytes.toBytes(ts)),
                                                        new Pair<>(Bytes.toBytes("is_supplement"), Bytes.toBytes("0")),
                                                        new Pair<>(Bytes.toBytes("obd_protocol"), Bytes.toBytes(value.getByte("OBDProtocol").toString())),
                                                        new Pair<>(Bytes.toBytes("mil_state"), Bytes.toBytes(value.getByte("MILState").toString())),
                                                        new Pair<>(Bytes.toBytes("diagnostics_support"), Bytes.toBytes(value.getShort("DiagnosticsSupport").toString())),
                                                        new Pair<>(Bytes.toBytes("diagnostics_ready"), Bytes.toBytes(value.getShort("DiagnosticsReady").toString())),
                                                        new Pair<>(Bytes.toBytes("vin2"), Bytes.toBytes(value.getString("VIN").toString())),
                                                        new Pair<>(Bytes.toBytes("soft_cin"), Bytes.toBytes(value.getString("SoftCIN").toString())),
                                                        new Pair<>(Bytes.toBytes("soft_cvn"), Bytes.toBytes(value.getString("SoftCVN").toString())),
                                                        new Pair<>(Bytes.toBytes("iupr"), Bytes.toBytes(value.getString("IUPR").toString())),
                                                        new Pair<>(Bytes.toBytes("fault_code_count"), Bytes.toBytes(value.getByte("FaultCodeCount").toString())),
                                                        new Pair<>(Bytes.toBytes("fault_code_list"), Bytes.toBytes(value.getString("FaultCodeList"))));

                                                String rowKey = Utils.generateRowKey2(vin);
                                                HBaseUtils.putRow2(TABLE_NAME, rowKey, DESERIALIZE, pairs1);
                                                System.out.printf(rowKey);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        else if (msgId.equals("3"))
                        {
                            if (jsonObject.containsKey("Bodies")) {
                                JSONObject bodies = JSON.parseObject(jsonObject.getString("Bodies"));
                                if (bodies.containsKey("Supplement")) {
                                    JSONObject sup = JSON.parseObject(bodies.getString("Supplement"));
                                    if (sup.containsKey("ValueList")) {
                                        JSONArray valueList = JSON.parseArray(sup.getString("ValueList"));
                                        for (Object obj: valueList) {
                                            JSONObject value = (JSONObject) obj;
                                            if (value.containsKey("TypeCode"))
                                            {
                                                if (value.getInteger("TypeCode") == 2)
                                                {
                                                    DateTime dateTime = new DateTime(value.getString("GpsTime"), DatePattern.NORM_DATETIME_FORMAT);
                                                    long ts = dateTime.toTimestamp().getTime();
                                                    List<Pair<byte[], byte[]>> pairs1 = Arrays.asList(new Pair<>(Bytes.toBytes("vin"), Bytes.toBytes(vin)),
                                                            new Pair<>(Bytes.toBytes("typecode"), Bytes.toBytes(vin)),
                                                            new Pair<>(Bytes.toBytes("gps_time"), Bytes.toBytes(ts)),
                                                            new Pair<>(Bytes.toBytes("is_supplement"), Bytes.toBytes("1")),
                                                            new Pair<>(Bytes.toBytes("speed"), Bytes.toBytes(value.getDouble("Speed").toString())),
                                                            new Pair<>(Bytes.toBytes("pressure"), Bytes.toBytes(value.getDouble("Pressure").toString())),
                                                            new Pair<>(Bytes.toBytes("net_output_torque"), Bytes.toBytes(value.getShort("NetOutputTorque").toString())),
                                                            new Pair<>(Bytes.toBytes("friction_torque"), Bytes.toBytes(value.getShort("FrictionTorque").toString())),
                                                            new Pair<>(Bytes.toBytes("rpm"), Bytes.toBytes(value.getDouble("RPM").toString())),
                                                            new Pair<>(Bytes.toBytes("fuel_flow"), Bytes.toBytes(value.getDouble("FuelFlow").toString())),
                                                            new Pair<>(Bytes.toBytes("scr_up_nox"), Bytes.toBytes(value.getDouble("SCRUpNOx").toString())),
                                                            new Pair<>(Bytes.toBytes("scr_down_nox"), Bytes.toBytes(value.getDouble("SCRDownNOx").toString())),
                                                            new Pair<>(Bytes.toBytes("reagent_allowance"), Bytes.toBytes(value.getDouble("ReagentAllowance").toString())),
                                                            new Pair<>(Bytes.toBytes("air_inflow"), Bytes.toBytes(value.getDouble("AirInflow").toString())),
                                                            new Pair<>(Bytes.toBytes("scr_inlet_temp"), Bytes.toBytes(value.getDouble("SCRInletTemp").toString())),
                                                            new Pair<>(Bytes.toBytes("scr_outlet_temp"), Bytes.toBytes(value.getDouble("SCROutletTemp").toString())),
                                                            new Pair<>(Bytes.toBytes("dpf"), Bytes.toBytes(value.getDouble("DPF").toString())),
                                                            new Pair<>(Bytes.toBytes("coolant_temp"), Bytes.toBytes(value.getShort("CoolantTemp").toString())),
                                                            new Pair<>(Bytes.toBytes("fuel_tank_level"), Bytes.toBytes(value.getDouble("FuelTankLevel").toString())),
                                                            new Pair<>(Bytes.toBytes("locate_statu"), Bytes.toBytes(value.getDouble("LocateStatu").toString())),
                                                            new Pair<>(Bytes.toBytes("lng"), Bytes.toBytes(value.getDouble("Lng").toString())),
                                                            new Pair<>(Bytes.toBytes("lat"), Bytes.toBytes(value.getDouble("Lat").toString())),
                                                            new Pair<>(Bytes.toBytes("mileage"), Bytes.toBytes(value.getDouble("Mileage").toString())));

                                                    //String rowKey = Utils.generateRowKey3(vin, ts);
                                                    String rowKey = Utils.generateRowKey2(vin);
                                                    HBaseUtils.putRow2(TABLE_NAME, rowKey, DESERIALIZE, pairs1);
                                                    System.out.printf(rowKey);
                                                }
                                                else if (value.getInteger("TypeCode") == 1)
                                                {
                                                    DateTime dateTime = new DateTime(value.getString("GpsTime"), DatePattern.NORM_DATETIME_FORMAT);
                                                    long ts = dateTime.toTimestamp().getTime();
                                                    List<Pair<byte[], byte[]>> pairs1 = Arrays.asList(new Pair<>(Bytes.toBytes("vin"), Bytes.toBytes(vin)),
                                                            new Pair<>(Bytes.toBytes("gps_time"), Bytes.toBytes(ts)),
                                                            new Pair<>(Bytes.toBytes("is_supplement"), Bytes.toBytes("1")),
                                                            new Pair<>(Bytes.toBytes("obd_protocol"), Bytes.toBytes(value.getByte("OBDProtocol").toString())),
                                                            new Pair<>(Bytes.toBytes("mil_state"), Bytes.toBytes(value.getByte("MILState").toString())),
                                                            new Pair<>(Bytes.toBytes("diagnostics_support"), Bytes.toBytes(value.getShort("DiagnosticsSupport").toString())),
                                                            new Pair<>(Bytes.toBytes("diagnostics_ready"), Bytes.toBytes(value.getShort("DiagnosticsReady").toString())),
                                                            new Pair<>(Bytes.toBytes("vin2"), Bytes.toBytes(value.getString("VIN").toString())),
                                                            new Pair<>(Bytes.toBytes("soft_cin"), Bytes.toBytes(value.getString("SoftCIN").toString())),
                                                            new Pair<>(Bytes.toBytes("soft_cvn"), Bytes.toBytes(value.getString("SoftCVN").toString())),
                                                            new Pair<>(Bytes.toBytes("iupr"), Bytes.toBytes(value.getString("IUPR").toString())),
                                                            new Pair<>(Bytes.toBytes("fault_code_count"), Bytes.toBytes(value.getByte("FaultCodeCount").toString())),
                                                            new Pair<>(Bytes.toBytes("fault_code_list"), Bytes.toBytes(value.getString("FaultCodeList"))));

                                                    String rowKey = Utils.generateRowKey2(vin);
                                                    HBaseUtils.putRow2(TABLE_NAME, rowKey, DESERIALIZE, pairs1);
                                                    System.out.printf(rowKey);
                                                }
                                            }
                                        }
                                    }
                                }

                            }
                        }


                    }
                }

                 /*同步提交*/
                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }

    }
}

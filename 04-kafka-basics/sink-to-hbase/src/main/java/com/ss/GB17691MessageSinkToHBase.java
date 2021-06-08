package com.ss;

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
public class GB17691MessageSinkToHBase {
    private static final String TABLE_NAME = "gb17691_message";
    private static final String ORIGNAL = "o";

    public static void main(String[] args) {
        String topic = "gb17691_message";
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
                    //System.out.printf("topic = %s,partition = %d, key = %s, value = %s, offset = %d,\n",
                    //        record.topic(), record.partition(), record.key(), record.value(), record.offset());

                    //String vin = "h01aygtest0000003";
                    byte msgid = Bytes.toBytes(record.value())[2];
                    //byte[] a = Bytes.toBytes(record.key());
                    byte[] b = new byte[] { msgid };
                    //byte[] c = Bytes.toBytes(record.value());
                    List<Pair<byte[], byte[]>> pairs1 = Arrays.asList(new Pair<>(Bytes.toBytes("vin"), Bytes.toBytes(record.key())),
                            new Pair<>(Bytes.toBytes("msg_id"), b),
                            new Pair<>(Bytes.toBytes("data"), Bytes.toBytes(record.value())));

                    HBaseUtils.putRow2(TABLE_NAME, Utils.generateRowKey2(record.key()), ORIGNAL, pairs1);
                }
                 /*同步提交*/
                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }

    }
}

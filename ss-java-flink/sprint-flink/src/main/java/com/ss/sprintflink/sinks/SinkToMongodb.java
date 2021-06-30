package com.ss.sprintflink.sinks;

import com.alibaba.fastjson.JSONObject;
import com.ss.sprintflink.model.Gb32960TrackModel;
import com.ss.sprintflink.util.MongodbUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;
import scala.Tuple3;

import java.util.Map;

/**
 * Created by zhaozh on 2021/06/18.
 */
@Slf4j
public class SinkToMongodb  extends RichSinkFunction<Tuple3<String, Long, JSONObject>> {

    private MongoTemplate mongoTemplate;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        SpringApplication application = new SpringApplication(SinkToMongodb.class);
        ApplicationContext context = application.run(new String[]{});
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(Tuple3<String, Long, JSONObject> value, Context context) throws Exception {

        //MongodbUtils.updateMulti("vin", value._1(),  );
        Gb32960TrackModel gb32960TrackModel = new Gb32960TrackModel();
        gb32960TrackModel.setVin(value._1());
        gb32960TrackModel.setTrack(value._3());

        log.debug(gb32960TrackModel.toString());

        //mongoTemplate.insert(gb32960TrackModel);
        //MongodbUtils.saveOne("2342", gb32960TrackModel);
    }

}

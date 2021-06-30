package com.ss.sprintflink.model;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * Created by zhaozh on 2021/06/18.
 */
@Setter
@Getter
@Document(collection = "Gb32960Track")
public class Gb32960TrackModel {
    @Id //这个注解标识该字段对应集合里的每一行数据的mongo自定义生成的主键（16进制）
    private String objectId;

    @Field("vin") //使用该注解可以修改存入到mongo里的字段名
    private String vin;

    private JSONObject track;
}

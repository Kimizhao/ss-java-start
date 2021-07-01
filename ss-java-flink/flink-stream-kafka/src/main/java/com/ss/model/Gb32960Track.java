package com.ss.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

/**
 * Created by zhaozh on 2021/06/17.
 */
@Getter
@Setter
public class Gb32960Track {
    /* */
    private String vin;
    private Date gpsTime;
    private Byte isSupplement;
    private Byte carStatus;
    private Byte chargeStatus;
    private Byte operationMode;
    private Double speed;
    private Double mileage;
    private Double totalVoltage;
    private Double totalCurrent;
    private Byte soc;
    private Byte dcStatus;
    private Byte gear;
    private Integer ir;
    private String data;
}

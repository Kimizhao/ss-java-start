package com.ss.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
/**
 * Created by zhaozh on 2021/06/10.
 */
@Getter
@Setter
public class User implements Serializable {
    private static final long serialVersionUID = -3258839839160856613L;
    private Long id;
    private String userName;
    private String passWord;
}

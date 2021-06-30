package com.ss.sprintflink.model;

/**
 * Created by zhaozh on 2021/06/21.
 */
public class UserModel {
    private Integer id;
    private String name;

    public UserModel(Integer id, String name) {
        this.id = id;
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

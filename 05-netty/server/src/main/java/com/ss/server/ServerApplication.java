package com.ss.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.net.InetSocketAddress;

@SpringBootApplication
public class ServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ServerApplication.class, args);
        //启动服务端
        NettyServer nettyServer = new NettyServer();
        nettyServer.start(new InetSocketAddress("0.0.0.0", 8090));
    }

}

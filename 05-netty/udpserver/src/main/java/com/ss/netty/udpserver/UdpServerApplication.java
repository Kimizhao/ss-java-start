package com.ss.netty.udpserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class UdpServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(UdpServerApplication.class, args);
        UdpServer udpServer = new UdpServer();
        udpServer.start();
    }

}

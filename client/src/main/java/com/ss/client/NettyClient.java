package com.ss.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by zhaozh on 2021/04/26.
 *
 */
@Slf4j
public class NettyClient {

    public void start() {
        EventLoopGroup group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                //该参数的作用就是禁止使用Nagle算法，使用于小数据即时传输
                .option(ChannelOption.TCP_NODELAY, true)
                .channel(NioSocketChannel.class)
                .handler(new NettyClientInitializer());

        try {
            for (int i = 0;  i < 63000; i++) {
                try {
                    ChannelFuture channelFuture = bootstrap.connect("192.168.100.181", 8090);
                    channelFuture.addListener((ChannelFutureListener) future1 -> {
                        if (!future1.isSuccess()) {
                            System.out.println("connect failed, exit!");
                        }
                    });
                    channelFuture.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            ChannelFuture future = bootstrap.connect("192.168.100.181", 8090).sync();
            log.info("客户端成功....");
            //发送消息
            future.channel().writeAndFlush("你好啊");
            // 等待连接被关闭
            future.channel().closeFuture().sync();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            group.shutdownGracefully();
        }
    }
}

package com.ss.netty.udpclient;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

/**
 * Created by zhaozh on 2021/06/05.
 */
@Slf4j
public class UdpClientHandler extends SimpleChannelInboundHandler<DatagramPacket> {
    //接受服务端发送的内容
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
        log.info("客户端接收到消息：" + packet.content().toString(StandardCharsets.UTF_8));
        // 向客户端发送消息
        ByteBuf byteBuf = Unpooled.copiedBuffer("你好服务器".getBytes(StandardCharsets.UTF_8));
        ctx.writeAndFlush(new DatagramPacket(byteBuf, packet.sender()));
    }
}

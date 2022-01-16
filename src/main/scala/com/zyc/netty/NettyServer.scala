package com.zyc.netty

import com.typesafe.config.ConfigFactory
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ChannelInitializer, ChannelOption}
import io.netty.handler.codec.http._
import org.slf4j.LoggerFactory

class NettyServer{
  val logger=LoggerFactory.getLogger(this.getClass)
  val configLoader=ConfigFactory.load("application.conf")
  def start() = {
    val serverConf=configLoader.getConfig("server")
    val host=serverConf.getString("host")
    val port=serverConf.getInt("port")

    logger.info("netty server启动")
    this.bind(host,port)
  }


  def bind(host: String, port: Int): Unit = {
    //配置服务端线程池组
    //用于服务器接收客户端连接
    val bossGroup = new NioEventLoopGroup(1)
    //用户进行SocketChannel的网络读写
    val workerGroup = new NioEventLoopGroup(10)

    try {
      //是Netty用户启动NIO服务端的辅助启动类，降低服务端的开发复杂度
      val bootstrap = new ServerBootstrap()
      //将两个NIO线程组作为参数传入到ServerBootstrap
      bootstrap.group(bossGroup, workerGroup)
        //创建NioServerSocketChannel
        .channel(classOf[NioServerSocketChannel])
        //绑定I/O事件处理类
        .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          /*ch.pipeline().addLast(new HttpResponseEncoder());
            ch.pipeline().addLast(new HttpRequestDecoder());*/
          ch.pipeline().addLast(new HttpServerCodec());
          //HttpObjectAggregator解码器 将多个消息对象转换为full
          ch.pipeline().addLast("aggregator", new HttpObjectAggregator(512*1024))
          //压缩
          ch.pipeline().addLast("deflater", new HttpContentCompressor());
          ch.pipeline().addLast(new HttpServerHandler());
        }
      }).option[Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      //绑定端口，调用sync方法等待绑定操作完成
      val channelFuture = bootstrap.bind(port).sync()
      //等待服务关闭
      channelFuture.channel().closeFuture().sync()
    } finally {
      //优雅的退出，释放线程池资源
      bossGroup.shutdownGracefully()
      workerGroup.shutdownGracefully()
    }
  }
}

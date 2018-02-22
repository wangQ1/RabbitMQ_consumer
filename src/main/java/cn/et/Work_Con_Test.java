package cn.et;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * 消费者 —— 工作队列模式
 * @author Administrator
 *
 */
public class Work_Con_Test {

	/**  
     * 获取消息队列名称  
     */  
	private final static String QUEUE_NAME = "work_queue";
    /**
     * 异步接收
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
    	/**
         * 连接远程rabbitmq-server服务器
         */
    	//新建一个连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        //指定主机         端口默认为5672
        factory.setHost("192.168.48.128");
        //通过主机与端口获得连接
        Connection connection = factory.newConnection();
        //通过连接创建消息读写的通道
        final Channel channel = connection.createChannel();
        /**    定义创建一个队列
         * 消费者需要先定义队列      有可能消费者先于生产者启动
         * 参数2  true表示将队列和消息持久化到磁盘
         */
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        //定义回调函数抓取队列中的消息
        Consumer consumer = new DefaultConsumer(channel) {
			@Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                    byte[] body) throws IOException {
				System.out.println(new String(body, "UTF-8"));
				//参数2 true表示确认接受该队列所有消息  false只确认当前消息  每个消息都有一个消息标记
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        //接受消息，第二个参数    true表示自动确定接受   false表示手动确认接受   等接受者业务逻辑处理完成后 手动调用确认方法确认后 才会删除消息 否则消息就不会从服务器删除
        channel.basicConsume(QUEUE_NAME, false, consumer);
    }
}

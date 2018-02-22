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
public class Roult_Con_Test {

	/**  
     * 交换器名称  不允许使用  *.*格式
     */  
    private static final String EXCHANGE_NAME = "logs";
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
        /**    定义交换器
         * 参数1 交换器名字   参数2 交换器类型    参数3 true表示将交换器持久化到磁盘
         */
        channel.exchangeDeclare(EXCHANGE_NAME, "direct", true);
        //channel.basicQos(1);
        //产生一个随机的队列 该队列用于从交换器获取消息
        String queueName = channel.queueDeclare().getQueue();
        //将队列和某个交换机、routingkey绑定   就可以正式获取消息了  参数3 指定routingkey与生产者定义的routingkey匹配
        channel.queueBind(queueName, EXCHANGE_NAME, "error");
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        //定义回调抓取消息
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,  
                    byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
              //参数2 true表示确认接受该队列所有消息  false只确认当前消息  每个消息都有一个消息标记
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        //接受消息，第二个参数    true表示自动确定接受   false表示手动确认接受   等接受者业务逻辑处理完成后 手动调用确认方法确认后 才会删除消息 否则消息就不会从服务器删除
        channel.basicConsume(queueName, false, consumer);
    }
}

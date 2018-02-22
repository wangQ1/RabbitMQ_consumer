package cn.et;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
/**
 * 消费者 —— 简单模式
 * @author Administrator
 *
 */
@RestController
public class Simple_Con_Email {
	@Autowired
	private JavaMailSender jms;
	/**
	 * 字节数组反序列化为对象
	 * @param b
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private static Object reverseSerialize(byte[] b) throws IOException, ClassNotFoundException{
		ByteArrayInputStream bais = new ByteArrayInputStream(b);
		ObjectInputStream ois = new ObjectInputStream(bais);
		return ois.readObject();
	}
	/**  
     * 获取消息队列名称  
     */  
	private final static String QUEUE_NAME = "sendMail_queue";
    /**
     * 异步接收
     * @throws Exception
     */
	@RequestMapping("/sendMail")
    public String sendMail() throws Exception{
    	//新建一个连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        //指定主机         端口默认为5672
        factory.setHost("192.168.48.128");
        //通过主机与端口获得连接
        Connection connection = factory.newConnection();
        //通过连接创建消息读写的通道
        final Channel channel = connection.createChannel();
        //消费者需要先定义队列      有可能消费者先于生产者启动
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        //定义回调函数抓取队列中的消息
        Consumer consumer = new DefaultConsumer(channel) {
            @SuppressWarnings("unchecked")
			@Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                    byte[] body) throws IOException {
            	try {
					Map<String, String> map = (Map<String, String>)reverseSerialize(body);
					SimpleMailMessage smm = new SimpleMailMessage();
					//来自
					smm.setFrom(map.get("from"));
					//发送到
					smm.setTo(map.get("sendTo"));
					//标题
					smm.setSubject(map.get("subject"));
					//内容
					smm.setText(map.get("content"));
					jms.send(smm);
					//参数2 true表示确认接受该队列所有消息  false只确认当前消息  每个消息都有一个消息标记
	                channel.basicAck(envelope.getDeliveryTag(), false);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
            }
        };
        //接受消息，第二个参数    true表示自动确定接受   false表示手动确认接受   等接受者业务逻辑处理完成后 手动调用确认方法确认后 才会删除消息 否则消息就不会从服务器删除
        channel.basicConsume(QUEUE_NAME, false, consumer);
        return "成功";
    }
    /**  
     * 同步接收 消费者定时去抓取   
     * 该种方式已过期  
     * @throws Exception   
    public static void asyncFalseRec() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.48.105");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(QUEUE_NAME, true, consumer);
        while (true) {
             QueueingConsumer.Delivery delivery = consumer.nextDelivery();
             String message = new String(delivery.getBody());
             System.out.println(" [x] Received '" + message + "'");
        }
    }
    */
}

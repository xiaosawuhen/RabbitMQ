package lxzl.com.mq.client;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class MqClient2 {

	public static void main(String[] args) {
		MqClient2 client01 = new MqClient2("queue02");
		client01.start();
	}

	public MqClient2(String queueName) {
		this.queueName = queueName;
	}

	public static final String IP = "192.168.240.128";
	public static final int PORT = 5672;
	public static final String USERNAME = "admin";
	public static final String PASSWORD = "password";

	public static final String EXCHANGE = "hello-exchange";
	public static final String ROUTKEY01 = "routKey01";
	public String queueName = "queue01";
	
	public void start() {		bindInfo(EXCHANGE, ROUTKEY01, queueName);
	}
	
	public void bindInfo(String exchangeName, String routingKey, String queueName) {
		Connection conn = getConnection(IP, PORT, USERNAME, PASSWORD);
		
		// 获得信道
		Channel channel = null;
		try {
			channel = conn.createChannel();
			// 声明交换器
			channel.exchangeDeclare(exchangeName, "direct", true);
			// 声明队列
			channel.queueDeclare(queueName, false, false, false, null);
			// 绑定队列，通过键 hola 将队列和交换器绑定起来
			channel.queueBind(queueName, exchangeName, routingKey);

			while (true) {
				// 消费消息
				boolean autoAck = false;
				String consumerTag = "";
				channel.basicConsume(queueName, autoAck, consumerTag, getConsumer(channel));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	
	}

	public Consumer getConsumer(Channel channel) {
		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				String routingKey = envelope.getRoutingKey();
				String contentType = properties.getContentType();
				System.out.println("消费的路由键：" + routingKey);
				System.out.println("消费的内容类型：" + contentType);
				long deliveryTag = envelope.getDeliveryTag();
				
				// 确认消息
				channel.basicAck(deliveryTag, false);
				System.out.println("消费的消息体内容：");
				String bodyStr = new String(body, "UTF-8");
				System.out.println(bodyStr);
			}
		};

		return consumer;
	}

	public Connection getConnection(String ip, int port, String username, String password) {

		// 创建连接工厂
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUsername(username);
		factory.setPassword(password);
		// 设置 RabbitMQ 地址
		factory.setHost(ip);
		factory.setPort(port);
		// 建立到代理服务器到连接
		Connection conn = null;
		try {
			conn = factory.newConnection();
		} catch (IOException | TimeoutException e) {
			e.printStackTrace();
		}

		return conn;
	}
}

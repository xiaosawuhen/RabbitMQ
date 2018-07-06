package lxzl.com.mq.server;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class MqServer {

	public static void main(String[] args) throws IOException, TimeoutException {
		MqServer mqServer = new MqServer();
		mqServer.pushMessageWithExchange("hello-exchange", "routKey01", "testExchange");
	}

	public static final String IP = "192.168.240.128";
	public static final int PORT = 5672;
	public static final String USERNAME = "admin";
	public static final String PASSWORD = "password";

	public void pushMessageWithExchange(String exchangeName, String routingKey, String message) {
		Connection conn = getConnection(IP, PORT, USERNAME, PASSWORD);

		Channel channel = null;
		try {
			// 获得信道
			channel = conn.createChannel();
			
			// 声明交换器
			channel.exchangeDeclare(exchangeName, "direct", true);

			// 发布消息
			channel.basicPublish(exchangeName, routingKey, null, message.getBytes());

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (channel != null) {
					channel.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (IOException | TimeoutException e) {
				e.printStackTrace();
			}
		}
	}

	public void pushMessageWithQuere(String queueName, String message) {
		Connection conn = getConnection(IP, PORT, USERNAME, PASSWORD);

		Channel channel = null;
		try {
			// 获得信道
			channel = conn.createChannel();
			channel.queueDeclare(queueName, false, false, false, null);

			// 发布消息
			channel.basicPublish("", queueName, null, message.getBytes("UTF-8"));
			System.out.println(" [x] Sent '" + message + "'");

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (channel != null) {
					channel.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (IOException | TimeoutException e) {
				e.printStackTrace();
			}
		}
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

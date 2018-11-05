using System;
using System.Collections.Concurrent;
using System.Text;

namespace RpcClientApp
{
    using RabbitMQ.Client;
    using RabbitMQ.Client.Events;

    /// <summary>
    /// RPC客户端
    /// </summary>
    public class RpcClient
    {
        private readonly IConnection connection; //连接
        private readonly IModel channel; //信道
        private readonly string replyQueueName; //回调队列
        private readonly EventingBasicConsumer consumer; //消费者
        private readonly BlockingCollection<string> respQueue = new BlockingCollection<string>(); //线程安全的集合
        private readonly IBasicProperties props; //基本属性

        public RpcClient()
        {
            //连接工厂
            var factory = new ConnectionFactory() { HostName = "localhost" };

            connection = factory.CreateConnection(); //建立连接
            channel = connection.CreateModel(); //开启信道（虚拟连接）
            replyQueueName = channel.QueueDeclare().QueueName; //申明一个匿名队列。
            consumer = new EventingBasicConsumer(channel); //消费者，用于监听响应的消息

            props = channel.CreateBasicProperties();
            var correlationId = Guid.NewGuid().ToString(); //使用guid作为请求标识号
            props.CorrelationId = correlationId;
            props.ReplyTo = replyQueueName; //设置回调队列

            //接收消息处理
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var response = Encoding.UTF8.GetString(body);
                if (ea.BasicProperties.CorrelationId == correlationId)
                {
                    //如果接收到的消息，是响应的消息，则把响应的消息体加入响应列表。
                    respQueue.Add(response);
                }
            };
        }

        public string Call(string message)
        {
            var messageBytes = Encoding.UTF8.GetBytes(message);

            //发送消息
            channel.BasicPublish(exchange: "", //设置空字符串，消息将发送到默认的交换器中。
                                 routingKey: "rpc_queue", // 路由键，交换器根据路由键将消息存储到相应的队列之中。
                                 basicProperties: props, //消息属性集。设置了请求标识号，和消息回调队列。
                                 body: messageBytes); //消息体。

            //订阅消息
            channel.BasicConsume(consumer: consumer, //处理回调消息的消费者。
                                 queue: replyQueueName, //回调消息队列。
                                 autoAck: true); //消息自动确认。

            //获得消息响应的信息
            return respQueue.Take();
        }

        public void Close()
        {
            connection.Close();
        }
    }

    public class Rpc
    {
        public static void Main()
        {
            var rpcClient = new RpcClient();

            Console.WriteLine(" [x] Requesting fib(30)");
            var response = rpcClient.Call("30");

            Console.WriteLine(" [.] Got '{0}'", response);
            rpcClient.Close();
        }
    }
}

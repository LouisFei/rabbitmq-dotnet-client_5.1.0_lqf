using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ReceiveLogsApp
{
    using System;
    using System.Text;

    class ReceiveLogs
    {
        public static void Main()
        {
            //连接工厂
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using (var connection = factory.CreateConnection()) //创建连接
            {
                using (var channel = connection.CreateModel()) //创建信道
                {
                    //声明一个fanout类型的交换器。
                    //fanout类型的交换器，会把所有发送到该交换器的消息路由到所有与该交换器绑定的队列中。
                    channel.ExchangeDeclare(exchange: "logs", //交换器名称
                                            type: "fanout");  //交换器类型，fanout类型会无条件把消息发送给所有绑定到此交换器上的队列。

                    //申明一个队列。
                    // 不带任何参数，默认创建一个RabbitMQ命名的（类似这种amq.gen-LhQz1gv3GhDOv8PIDabOXA 名称，这种队列也称之为匿名队列）
                    //               排他的、自动删除的、非持久化的队列。
                    var queueName = channel.QueueDeclare().QueueName; 

                    //将队列和交换器绑定。
                    channel.QueueBind(queue: queueName, //队列名称
                                      exchange: "logs", //交换器名称
                                      routingKey: ""); //用来绑定队列和交换器的路由键。

                    Console.WriteLine(" [*] Waiting for logs.");

                    var consumer = new EventingBasicConsumer(channel);

                    //接收到消息，进行相应处理
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body); //utf8解码
                        Console.WriteLine(" [x] {0}", message);
                    };

                    //订阅消息
                    channel.BasicConsume(queue: queueName, //消息队列名称
                                         autoAck: true, //消息自动确认
                                         consumer: consumer); //消费者

                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }
    }
}

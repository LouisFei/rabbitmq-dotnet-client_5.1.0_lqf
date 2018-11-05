using System;
using System.Text;

namespace ReceiveLogsDirectApp
{
    using RabbitMQ.Client;
    using RabbitMQ.Client.Events;

    class ReceiveLogsDirect
    {
        public static void Main(string[] args)
        {
            //连接工厂
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using (var connection = factory.CreateConnection()) //创建连接
            {
                using (var channel = connection.CreateModel()) //创建信道
                {
                    //声明一个direct类型的交换器。
                    //direct类型的交换器，通过路由规则与匹配的队列进行绑定。
                    //它会把消息路由到那些BindingKey和RoutingKey完全匹配的队列中。
                    channel.ExchangeDeclare(exchange: "direct_logs", //交换器名称
                                            type: "direct"); //交换器类型

                    //申明一个队列。
                    // 不带任何参数，默认创建一个RabbitMQ命名的（类似这种amq.gen-LhQz1gv3GhDOv8PIDabOXA 名称，这种队列也称之为匿名队列）
                    //               排他的、自动删除的、非持久化的队列。
                    var queueName = channel.QueueDeclare().QueueName;

                    if (args.Length < 1)
                    {
                        Console.Error.WriteLine("Usage: {0} [info] [warning] [error]",
                                                Environment.GetCommandLineArgs()[0]);
                        Console.WriteLine(" Press [enter] to exit.");
                        Console.ReadLine();
                        Environment.ExitCode = 1;
                        return;
                    }

                    foreach (var severity in args)
                    {
                        //将队列和交换器绑定。
                        channel.QueueBind(queue: queueName, //队列名称
                                          exchange: "direct_logs", //交换器名称
                                          routingKey: severity); //用来绑定队列和交换器的路由键。
                    }

                    Console.WriteLine(" [*] Waiting for messages.");

                    var consumer = new EventingBasicConsumer(channel);

                    //接收到消息，进行相应处理
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        var routingKey = ea.RoutingKey;
                        Console.WriteLine(" [x] Received '{0}':'{1}'",
                                          routingKey, message);
                    };

                    //订阅消息
                    channel.BasicConsume(queue: queueName, //消除队列名称
                                         autoAck: true, //消息自动确认
                                         consumer: consumer); //消费者

                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }
    }
}

using System;
using System.Text;

namespace ReceiveLogsTopicApp
{
    using RabbitMQ.Client;
    using RabbitMQ.Client.Events;

    class ReceiveLogsTopic
    {
        public static void Main(string[] args)
        {
            //连接工厂
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using (var connection = factory.CreateConnection()) //创建连接
            {
                using (var channel = connection.CreateModel()) //创建信道
                {
                    //声明一个topic类型的交换器。
                    //topic类型的交换器，在匹配规则上进行了模式匹配。
                    //RoutingKey为一个点号"."分隔的字符串（被点号"."分隔开的每一段独立的字符串称为一个单词）；
                    //BindingKey和RoutingKey一样也是点号"."分隔的字符串；
                    //BindingKey中可以存在两种特殊字符串"*"和"#"，用于做模糊匹配，其中"*"用于匹配一个单词，"#"用于匹配多个单词（可以是零个）。
                    channel.ExchangeDeclare(exchange: "topic_logs", //交换器名称
                                            type: "topic"); //交换器类型

                    //申明一个队列。
                    // 不带任何参数，默认创建一个RabbitMQ命名的（类似这种amq.gen-LhQz1gv3GhDOv8PIDabOXA 名称，这种队列也称之为匿名队列）
                    //               排他的、自动删除的、非持久化的队列。
                    var queueName = channel.QueueDeclare().QueueName;

                    if (args.Length < 1)
                    {
                        Console.Error.WriteLine("Usage: {0} [binding_key...]",
                                                Environment.GetCommandLineArgs()[0]);
                        Console.WriteLine(" Press [enter] to exit.");
                        Console.ReadLine();
                        Environment.ExitCode = 1;
                        return;
                    }

                    foreach (var bindingKey in args)
                    {
                        //将队列和交换器绑定。
                        channel.QueueBind(queue: queueName, //队列名称
                                          exchange: "topic_logs", //交换器名称
                                          routingKey: bindingKey); //用来绑定队列和交换器的路由键。
                    }

                    Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");

                    var consumer = new EventingBasicConsumer(channel);

                    //接收到消息，进行相应处理
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        var routingKey = ea.RoutingKey;
                        Console.WriteLine(" [x] Received '{0}':'{1}'",
                                          routingKey,
                                          message);
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

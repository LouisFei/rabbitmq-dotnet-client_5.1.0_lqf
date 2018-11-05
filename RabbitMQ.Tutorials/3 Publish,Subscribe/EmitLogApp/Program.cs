using System;
using System.Text;

namespace EmitLogApp
{
    using RabbitMQ.Client;

    class EmitLog
    {
        public static void Main(string[] args)
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
                                            type: "fanout"); //交换器类型

                    //消息内容
                    var message = GetMessage(args);
                    var body = Encoding.UTF8.GetBytes(message);

                    //发送消息
                    channel.BasicPublish(exchange: "logs", //交换器
                                         routingKey: "", //路由键，交换器根据路由键将消息存储到相应的队列之中。因为使用的交换器类型是fanout，路由键的设置将失效。
                                         basicProperties: null, //消息的基本属性集。
                                         body: body); //消息体（payload），真正需要发送的消息。

                    Console.WriteLine(" [x] Sent {0}", message);
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        private static string GetMessage(string[] args)
        {
            return ((args.Length > 0)
                   ? string.Join(" ", args)
                   : "info: Hello World!");
        }
    }
}

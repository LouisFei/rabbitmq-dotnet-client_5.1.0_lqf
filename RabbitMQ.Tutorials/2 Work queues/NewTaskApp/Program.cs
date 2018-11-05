using System;
using System.Text;

namespace NewTaskApp
{
    using RabbitMQ.Client;

    class NewTask
    {
        public static void Main(string[] args)
        {
            //连接工厂
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using (var connection = factory.CreateConnection()) //创建连接
            {
                using (var channel = connection.CreateModel()) //创建信道
                {
                    //创建队列
                    channel.QueueDeclare(queue: "task_queue", //队列的名称
                                         durable: true, //设置队列持久化。
                                         exclusive: false, //设置队列非排他。
                                         autoDelete: false, //设置队列非自动删除。
                                         arguments: null); //设置队列其它的参数。

                    //消息内容
                    var message = GetMessage(args);
                    //消息内容进行utf8编码为字节数组
                    var body = Encoding.UTF8.GetBytes(message);

                    //基本属性集
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;

                    //发送消息
                    channel.BasicPublish(exchange: "", //交换器的名称，指明消息需要发送到哪个交换器中。如果设置为空字符串，则消息会被发送到RabbitMQ默认的交换器中。
                                         routingKey: "task_queue", //路由键，交换器根据路由键将消息存储到相应的队列之中。
                                         basicProperties: properties, //消息的基本属性集。
                                         body: body);//消息体（payload），真正需要发送的消息。

                    Console.WriteLine(" [x] Sent {0}", message);
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        private static string GetMessage(string[] args)
        {
            return ((args.Length > 0) ? string.Join(" ", args) : "Hello World!");
        }
    }
}

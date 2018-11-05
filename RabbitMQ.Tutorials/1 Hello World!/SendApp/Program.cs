using System;
using System.Text;

namespace SendApp
{
    using RabbitMQ.Client;

    class Send
    {
        public static void Main()
        {
            //连接工厂
            var factory = new ConnectionFactory() { HostName = "localhost" };

            /*
             默认情况下，访问RabbitMQ服务的用户名和密码都是“guest”，
             这个账户有限制，默认只能通过本地网络访问（如localhost），远程网络访问受限。
             RabbitMQ服务端默认端口号为5672
             */

            using (var connection = factory.CreateConnection()) //创建连接
            {
                using (var channel = connection.CreateModel()) //创建信道
                {
                    //创建队列
                    channel.QueueDeclare(queue: "hello", //设置队列的名称。
                                         durable: false, //设置队列非持久化。持久性的队列可以存盘，在服务器重启的时候可以保证不丢失相关消息。
                                         exclusive: false, //设置队列非排他的。如果一个队列被声明为排他队列，该队列仅对首次声明它的连接可见，并在连接断开时自动删除，此时持久化设置是无效的。
                                         autoDelete: false, //设置队列非自动删除的。
                                         arguments: null); //设置队列其它的参数。

                    string message = string.Format("Hello World! {0}", DateTime.Now); //消息内容
                    var body = Encoding.UTF8.GetBytes(message); //将消息内容进行utf-8编码成字节数组

                    //发送消息
                    channel.BasicPublish(exchange: "", //交换器的名称，指明消息需要发送到哪个交换器中。如果设置为空字符串，则消息会被发送到RabbitMQ默认的交换器中。
                                         routingKey: "hello", //路由键，交换器根据路由键将消息存储到相应的队列之中。
                                         basicProperties: null, //消息的基本属性集。
                                         body: body); //消息体（payload），真正需要发送的消息。

                    Console.WriteLine(" [x] Sent {0}", message);
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}

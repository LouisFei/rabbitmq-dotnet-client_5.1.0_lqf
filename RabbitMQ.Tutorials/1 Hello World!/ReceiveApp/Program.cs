using System;
using System.Text;

namespace ReceiveApp
{
    using RabbitMQ.Client;
    using RabbitMQ.Client.Events;

    class Receive
    {
        public static void Main()
        {
            //连接工厂
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection()) //建立连接
            {
                using (var channel = connection.CreateModel()) //开启信道（虚拟连接）
                {
                    //创建队列
                    channel.QueueDeclare(queue: "hello", //消息队列名称
                                         durable: false, //消息队列非持久化
                                         exclusive: false, //消息队列非排它
                                         autoDelete: false, //消息队列非自动删除
                                         arguments: null); 

                    //创建消息消费者
                    var consumer = new EventingBasicConsumer(channel);

                    //监听消息消费者获取消息的事件，并进行相关处理
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" [x] Received {0}", message);
                    };

                    //消费模式使用推（push）模式，通过持续订阅的方式来消费消息。
                    channel.BasicConsume(queue: "hello", //队列的名称
                                         autoAck: true, //设置为自动确认。建议设成false，即不自动确认。然后在接收到消息之后进行显式ack操作，可以防止消息不必要地丢失。
                                         consumer: consumer);//消费者

                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }
    }
}

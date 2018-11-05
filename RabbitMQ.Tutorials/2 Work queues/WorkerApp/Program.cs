using System;
using System.Text;
using System.Threading;

namespace WorkerApp
{
   using RabbitMQ.Client;
    using RabbitMQ.Client.Events;

    class Worker
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
                    channel.QueueDeclare(queue: "task_queue", //消息队列名称
                                         durable: true, //消息队列持久化
                                         exclusive: false, //消息队列非排它
                                         autoDelete: false, //消息队列非自动删除
                                         arguments: null);

                    //限制信道上的消费者所能保持的最大未确认信息的数量。
                    channel.BasicQos(prefetchSize: 0, //表示消费者所能接收未确认消息的总体大小的上限，单位为B，设置为0则表示没有上限。
                                     prefetchCount: 1, //预取个数。0表示没有上限。
                                     global: false); //false，信道上新的消费者需要遵从perfetchCount的限定值。
                                                     //true， 信道上所有的消费者都需要遵从perfetchCount的限定值。

                    Console.WriteLine(" [*] Waiting for messages.");

                    //消费者
                    var consumer = new EventingBasicConsumer(channel);

                    //消费者监听接收消息事件
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" [x] Received {0}", message);

                        //模拟任务处理耗时
                        int dots = message.Split('.').Length - 1;
                        Thread.Sleep(dots * 1000);

                        Console.WriteLine(" [x] Done");

                        //显式确认消息接收到
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, //该消息的index，即消息的编号。
                                         multiple: false); //是否批量。true将一次性确认所有小于deliveryTag的消息。
                    };

                    //订阅消息
                    channel.BasicConsume(queue: "task_queue", //队列名称。
                                         autoAck: false, //设置消息不自动确认。
                                         consumer: consumer); //消费者

                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }
    }
}

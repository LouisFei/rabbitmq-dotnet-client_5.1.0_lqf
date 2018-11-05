using System;
using System.Text;

namespace RPCServerApp
{
    using RabbitMQ.Client;
    using RabbitMQ.Client.Events;

    class RPCServer
    {
        public static void Main()
        {
            //连接工厂
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using (var connection = factory.CreateConnection()) //创建连接
            {
                using (var channel = connection.CreateModel()) //创建信道
                {
                    //申明一个队列.
                    channel.QueueDeclare(queue: "rpc_queue",  //设置队列的名称
                                         durable: false,      // 设置队列非持久化
                                         exclusive: false,    //设置队列非排他的
                                         autoDelete: false,   //设置队列非自动删除的
                                         arguments: null);    //设置队列其它的参数

                    //设置消费者客户端最大能“保持”的未确认的消息数（即预取个数）。
                    //限制信道上的消费者所能保持的最大未确认消息的数量。
                    //对于拉模式的消费方式无效。
                    channel.BasicQos(prefetchSize: 0,  //表示消费者所能接收未确认消息的总体大小的上限，单位为B，设置为0则表示没有上限。
                                     prefetchCount: 1, //表示消费者所能接收未确认消息个数的上限，设置为0则表示没有上限。
                                     global: false);   //false：信道上 "新的"   消费者   需要遵从prefetchCount的限定值。
                                                       //true： 信道上 "所有的" 消费者 都需要遵从prefetchCount的限定值。
                    //对于一个信道来说，它可以同时消费多个队列，当设置了prefetchCount大于0时，
                    //这个信道要和各个队列协调以确保发送的消息都没有超过所限定的prefetchCount的值，这样会使RabbitMQ的性能降低。

                    //申明消费者
                    var consumer = new EventingBasicConsumer(channel);
                    //订阅消息
                    channel.BasicConsume(queue: "rpc_queue", //消息队列名称
                                         autoAck: false, //消息自动确认
                                         consumer: consumer); //消费者

                    Console.WriteLine(" [x] Awaiting RPC requests");

                    //接收到消息，进行相应处理
                    consumer.Received += (model, ea) =>
                    {
                        string response = null;

                        var body = ea.Body; //消息体
                        var props = ea.BasicProperties; //消息头（基本属性）

                        var replyProps = channel.CreateBasicProperties();
                        replyProps.CorrelationId = props.CorrelationId; 
                        //correlationId用来标记一个请求。表示此次请求的标识号，服务器处理完成后需要将此属性返还，
                        //客户端将根据这个id了解哪条请求被成功执行了或执行失败。

                        try
                        {
                            var message = Encoding.UTF8.GetString(body);
                            int n = int.Parse(message);
                            Console.WriteLine(" [.] fib({0})", message);
                            response = fib(n).ToString();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(" [.] " + e.Message);
                            response = "";
                        }
                        finally
                        {
                            //准备响应的消息
                            var responseBytes = Encoding.UTF8.GetBytes(response);

                            //发送响应消息（到回调队列，供客户端调用）
                            channel.BasicPublish(exchange: "", //交换器的名称，指明消息需要发送到哪个交换器中。如果设置为空字符串，则消息会被发送到RabbitMQ默认的交换器中。
                                                 routingKey: props.ReplyTo, //replyTo用来告知RPC服务器回复请求时的目的队列，即回调队列。
                                                 basicProperties: replyProps, //消息的基本属性集。（包含了一个RPC请求标识号）
                                                 body: responseBytes); //响应的消息
                            //确认消息接收到了
                            channel.BasicAck(deliveryTag: ea.DeliveryTag, //消息编号
                                             multiple: false); //是否批量。true将一次性确认所有小于deliveryTag的消息。
                        }
                    };

                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }

        //远程过程/远程服务
        /// 
        /// Assumes only valid positive integer input.
        /// Don't expect this one to work for big numbers, and it's
        /// probably the slowest recursive implementation possible.
        /// 
        private static int fib(int n)
        {
            if (n == 0 || n == 1)
            {
                return n;
            }

            return fib(n - 1) + fib(n - 2);
        }
    }
}

using System;
using System.Linq;
using System.Text;

namespace EmitLogTopicApp
{
    using RabbitMQ.Client;

    class EmitLogTopic
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

                    var routingKey = (args.Length > 0) ? args[0] : "anonymous.info";

                    var message = (args.Length > 1)
                                  ? string.Join(" ", args.Skip(1).ToArray())
                                  : "Hello World!";

                    var body = Encoding.UTF8.GetBytes(message);

                    //发送消息
                    channel.BasicPublish(exchange: "topic_logs", //交换器名称
                                         routingKey: routingKey, //路由键，交换器根据路由键将消息存储到相应的队列之中。
                                         basicProperties: null, //消息的基本属性集。
                                         body: body); //消息体（payload），真正需要发送的消息。

                    Console.WriteLine(" [x] Sent '{0}':'{1}'", routingKey, message);
                }
            }
        }
    }
}

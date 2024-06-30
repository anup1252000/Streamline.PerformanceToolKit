using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;
using RabbitMQ.Client;
using Streamline.PerformanceToolKit.RabbitMq;
using System.Text;
using System.Threading.Channels;

namespace Streamline.PerformanceToolkit.Test
{
    public class RabbitMqIntegration
    {
        [Fact]
        public async Task PublishMessage_ReturnBool()
        {
            var rabbitMqOption = new RabbitMqOptions
            {
                Host = "localhost",
                UserName = "guest",
                Password = "guest",
                Port = 5672,
                Exchange = "hello_direct_exchange",
                RoutingKey = "hello",
                Durable = true,
                ExchangeType = ExchangeType.Direct,
                Queue = "hello"
            };
            var options=Options.Create(rabbitMqOption);
            var message = "hello RabbitMQ anup";
            IMqService rabbitMqLibrary = new MqService(new AsyncChannelPoolWithDataflow(options,5),options);
            var result = await rabbitMqLibrary.PublishMessageAsync(message, rabbitMqOption);
            Assert.True(result);
        }

        [Fact]
        public async Task PublishMessageStream_ReturnInt()
        {
            var message = "hello RabbitMQ anup";
            var options = new RabbitMqOptions
            {
                Host = "localhost",
                UserName = "guest",
                Password = "guest",
                Port = 5672,
                Exchange = "",
                RoutingKey = "HelloWorld",
                Durable = true,
                ExchangeType = ExchangeType.Direct,
                Queue = "HelloWorld"
            };
            var someoptions = Options.Create(options);
            IMqService rabbitMqLibrary = new MqService(new AsyncChannelPoolWithDataflow(someoptions, 5), someoptions);
            var result = await rabbitMqLibrary.PublishStreamMessage(message, options);
            Assert.True(result);
        }

        [Fact]
        public async Task BulkPublish_ReturnTrue()
        {
            List<string> messages = new List<string>();
            for (int i = 0; i < 100000; i++)
            {
                messages.Add("Ganesh" + i);
            }
            var options = new RabbitMqOptions
            {
                Host = "localhost",
                UserName = "guest",
                Password = "guest",
                Port = 5672,
                Exchange = "hello_direct_exchange",
                RoutingKey = "hello",
                Durable = true,
                ExchangeType = ExchangeType.Direct,
                Queue = "hello"
            };

            var someoptions = Options.Create(options);
            IMqService rabbitMqLibrary = new MqService(new AsyncChannelPoolWithDataflow(someoptions, 5), someoptions);
            var result =await rabbitMqLibrary.BulkPublishAsync(messages, options);
            Assert.True(result);
        }

        [Fact]
        public async Task BulkPublishAsyncChannelPool_ReturnTrue()
        {

            List<string> messages = new List<string>();
            for (int i = 0; i < 100000; i++)
            {
                messages.Add("Parvati" + i);
            }

            var options = new RabbitMqOptions
            {
                Host = "localhost",
                UserName = "guest",
                Password = "guest",
                Port = 5672,
                Exchange = "hello_direct_exchange",
                RoutingKey = "hello",
                Durable = true,
                ExchangeType = ExchangeType.Direct,
                Queue = "hello"
            };

            var asyncChannel = new AsyncChannelPoolWithDataflow(Options.Create(options), 5);
            IModel channel = await asyncChannel.GetChannelAsync();

            try
            {
                channel.ConfirmSelect();
                var bulkPublish = channel.CreateBasicPublishBatch();

                channel.QueueDeclare(options.Queue, durable: true, exclusive: false, autoDelete: false);
                var properties = channel.CreateBasicProperties();
                properties.Persistent = true;

                foreach (var message in messages)
                {
                    var body = Encoding.UTF8.GetBytes(message).AsMemory();
                    bulkPublish.Add(options.Exchange, options.RoutingKey, false, properties, body);
                }

                bulkPublish.Publish();

                // Manually wait for confirms
                foreach (var message in messages)
                {
                    if (!channel.WaitForConfirms(TimeSpan.FromSeconds(10)))
                    {
                        // Handle failed confirmation
                        Console.WriteLine("Failed to confirm message: " + message);
                    }
                }
            }
            catch (Exception ex)
            {
                // Handle exceptions
                Console.WriteLine($"Error publishing messages: {ex.Message}");
            }
            finally
            {
                // Return the channel to the pool
                await asyncChannel.ReturnChannelAsync(channel);
            }


            #region Commented
            //List<string> messages = new List<string>();
            //for (int i = 0; i < 100000; i++)
            //{
            //    messages.Add("Parvati" + i);
            //}
            //var options = new RabbitMqOptions
            //{
            //    Host = "localhost",
            //    UserName = "guest",
            //    Password = "guest",
            //    Port = 5672,
            //    Exchange = "hello_direct_exchange",
            //    RoutingKey = "hello",
            //    Durable = true,
            //    ExchangeType = ExchangeType.Direct,
            //    Queue = "hello"
            //};
            //var asyncChannel=new AsyncChannelPoolWithDataflow(Options.Create(options),5);
            //IModel channel = await asyncChannel.GetChannelAsync();
            //try
            //{

            //    channel.ConfirmSelect();
            //    var bulkPublish = channel.CreateBasicPublishBatch();

            //    channel.QueueDeclare(options.Queue, durable: true, exclusive: false, autoDelete: false);
            //    var properties = channel.CreateBasicProperties();
            //    properties.Persistent = true;
            //    foreach (var message in messages)
            //    {
            //        var body = Encoding.UTF8.GetBytes(message).AsMemory();
            //        bulkPublish.Add(options.Exchange, options.RoutingKey, false, properties, body);
            //    }
            //    bulkPublish.Publish();

            //    //// Declare Exchange and Queue
            //    //channel.ExchangeDeclare(options.Exchange, options.ExchangeType, durable: options.Durable);
            //    //channel.QueueDeclare(options.Queue, durable: options.Durable, exclusive: false, autoDelete: false);
            //    //channel.QueueBind(options.Queue, options.Exchange, options.RoutingKey);

            //    //// Publish messages
            //    //foreach (var message in messages)
            //    //{
            //    //    var body = Encoding.UTF8.GetBytes(message);
            //    //    var properties = channel.CreateBasicProperties();
            //    //    properties.Persistent = true;

            //    //    channel.BasicPublish(options.Exchange, options.RoutingKey, properties, body);
            //    //}
            //}
            //finally
            //{
            //    await asyncChannel.ReturnChannelAsync(channel);
            //}
            #endregion
        }
    }

    #region Commented
    //public interface IRabbitMqService
    //{
    //    void PublishMessage(string message, RabbitMqOptions options);
    //}
    //public record RabbitMqOptions
    //{
    //    public string UserName { get; set; }
    //    public string Password { get; set; }
    //    public int Port { get; set; }
    //    public string Host { get; set; }
    //    public string Exchange { get; set; }
    //    public string RoutingKey { get; set; }
    //    public string Queue { get; set; }
    //    public bool Durable { get; set; }
    //    public string ExchangeType { get; set; }
    //}

    //public class RabbitMqService : IRabbitMqService
    //{
    //    public void PublishMessage(string message, RabbitMqOptions options)
    //    {
    //        var factory = new ConnectionFactory
    //        {
    //            HostName = options.Host,
    //            UserName = options.UserName,
    //            Password = options.Password,
    //            Port = options.Port
    //        };

    //        using (var connection = factory.CreateConnection())
    //        using (var channel = connection.CreateModel())
    //        {
    //            DeclareExchangeAndQueue(channel, options);

    //            var body = Encoding.UTF8.GetBytes(message);
    //            channel.BasicPublish(exchange: options.Exchange, routingKey: options.RoutingKey, basicProperties: null, body: body);
    //        }
    //    }

    //    private void DeclareExchangeAndQueue(IModel channel, RabbitMqOptions options)
    //    {
    //        channel.ExchangeDeclare(options.Exchange, options.ExchangeType, durable: options.Durable);
    //        channel.QueueDeclare(options.Queue, durable: options.Durable, exclusive: false, autoDelete: false, arguments: null);
    //        channel.QueueBind(options.Queue, options.Exchange, options.RoutingKey);
    //    }
    //}
    #endregion
}
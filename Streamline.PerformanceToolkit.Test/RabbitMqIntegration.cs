using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Streamline.PerformanceToolKit.RabbitMq;
using System.Text;

namespace Streamline.PerformanceToolkit.Test
{
    public class RabbitMqIntegration
    {
        private readonly RabbitMqOptions _rabbitMqOption;
        private readonly IOptions<RabbitMqOptions> _options;
        private readonly AsyncChannelPoolWithDataflow _channelPool;
        private readonly IMqService _rabbitMqLibrary;

        public RabbitMqIntegration()
        {
            _rabbitMqOption = new RabbitMqOptions
            {
                Host = "localhost",
                UserName = "guest",
                Password = "guest",
                Port = 5672,
                Exchange = "hello_direct_exchange",
                RoutingKey = "hello",
                Durable = true,
                ExchangeType = ExchangeType.Direct,
                Queue = "hello",
                ChannelPoolSize = 10
            };
            _options = Options.Create(_rabbitMqOption);
            _channelPool = new AsyncChannelPoolWithDataflow(_options);
            _rabbitMqLibrary = new MqService(_channelPool, _options, null); // Pass null for ILogger in tests
        }

        [Fact]
        public async Task PublishMessage_ReturnBool()
        {
            var message = "hello RabbitMQ anup";
            var result = await _rabbitMqLibrary.PublishMessageAsync(message);
            Assert.True(result);
        }

        [Fact]
        public async Task PublishStreamMessage_ReturnBool()
        {
            var message = "hello RabbitMQ anup";
            var streamOptions = new RabbitMqOptions
            {
                Host = "localhost",
                UserName = "guest",
                Password = "guest",
                Port = 5672,
                Exchange = "",
                RoutingKey = "HelloWorld",
                Durable = true,
                ExchangeType = ExchangeType.Direct,
                Queue = "HelloWorld",
                ChannelPoolSize = 5
            };

            var streamOptionsWrapper = Options.Create(streamOptions);
            var streamChannelPool = new AsyncChannelPoolWithDataflow(streamOptionsWrapper);
            var streamMqService = new MqService(streamChannelPool, streamOptionsWrapper, null); // Pass null for ILogger in tests

            var result = await streamMqService.PublishStreamMessageAsync(message);
            Assert.True(result);
        }

        [Fact]
        public async Task BulkPublish_ReturnTrue()
        {
            var messages = Enumerable.Range(0, 1000).Select(i => $"Ganesh{i}").ToList();
            var result = await _rabbitMqLibrary.BulkPublishAsync(messages);
            Assert.True(result);
        }

        [Fact]
        public async Task BulkPublishAsyncChannelPool_ReturnTrue()
        {
            var messages = Enumerable.Range(0, 1000).Select(i => $"Parvati{i}").ToList();

            IModel channel = await _channelPool.GetChannelAsync();
            try
            {
                channel.ConfirmSelect();
                var bulkPublish = channel.CreateBasicPublishBatch();

                channel.QueueDeclare(_rabbitMqOption.Queue, durable: true, exclusive: false, autoDelete: false);
                var properties = channel.CreateBasicProperties();
                properties.Persistent = true;

                foreach (var message in messages)
                {
                    var body = Encoding.UTF8.GetBytes(message).AsMemory();
                    bulkPublish.Add(_rabbitMqOption.Exchange, _rabbitMqOption.RoutingKey, false, properties, body);
                }

                bulkPublish.Publish();

                Assert.True(channel.WaitForConfirms(TimeSpan.FromSeconds(10)), "Failed to confirm messages");
            }
            finally
            {
                await _channelPool.ReturnChannelAsync(channel);
            }
        }
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
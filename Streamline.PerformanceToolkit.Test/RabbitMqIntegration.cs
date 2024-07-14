using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Streamline.PerformanceToolKit.Common;
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
        private readonly RabbitMQConsumer _consumer;
        private RabbitMQConsumerService _consumerService;

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
                ChannelPoolSize = 5
            };
            _options = Options.Create(_rabbitMqOption);
            _channelPool = new AsyncChannelPoolWithDataflow(_options);
            _rabbitMqLibrary = new MqService(_channelPool, _options, null); // Pass null for ILogger in tests
            _consumer = new RabbitMQConsumer(_options, _channelPool, null);

            // Create Polly retry policy
            var retryPolicy = Policy.Handle<Exception>()
                                    .WaitAndRetryAsync(
                                        _rabbitMqOption.RetryCount,
                                        attempt => TimeSpan.FromMilliseconds(_rabbitMqOption.RetryDelayMilliseconds),
                                        (exception, timeSpan, retryCount, context) =>
                                        {
                                            Console.WriteLine($"Retry {retryCount} encountered an error. Waiting {timeSpan.TotalMilliseconds}ms");
                                        });

            // var logger = new XunitLogger<RabbitMQConsumerService>();

            _consumerService = new RabbitMQConsumerService(_channelPool, _consumer, _options, new PollyRetryPolicy(_options, null), null);
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
                ChannelPoolSize = 5,
                PrefetchCount = 1000,
                BatchSize = 10000,
                BatchTimeout = 20000

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
            var messages = Enumerable.Range(0, 20000).Select(i => $"Ganesh{i}").ToList();
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

        [Fact]
        public async Task ConsumeMessages_Success()
        {

            var channel = await _channelPool.GetChannelAsync();
            try
            {
                 await _consumerService.StartAsync();
                //channel.BasicQos(prefetchSize: 0, prefetchCount: 1000, global: false);

                //var consumer = new EventingBasicConsumer(channel);
                //consumer.Received += async (sender, eventArgs) =>
                //{
                //    var message = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
                //    //  _logger.LogInformation("Received message: {Message}", message);

                //    // Process your message here...
                //    await ProcessMessageAsync(message);

                //    channel.BasicAck(eventArgs.DeliveryTag, multiple: false);
                //};

                //channel.BasicConsume(
                //    queue: _rabbitMqOption.Queue,
                //    autoAck: false,
                //    consumer: consumer);

                //_logger.LogInformation("Consumer started. Listening to queue: {Queue}", _options.Queue);

                // Wait until cancellation is requested
                await Task.Delay(Timeout.Infinite);
                await _consumerService.StopAsync();
            }
            catch (OperationCanceledException)
            {
                //_logger.LogInformation("Consumer stopped due to cancellation request.");
            }
            finally
            {
                await _channelPool.ReturnChannelAsync(channel);  
            }
           
        }



        private async Task ProcessMessageAsync(string message)
        {
            // Implement your message processing logic here
            await Task.Delay(TimeSpan.FromSeconds(1)); // Simulate processing time
                                                       //       _logger.LogInformation("Processed message: {Message}", message);
        }
    }
}

    //var message = "Test message";

    //// Start consuming messages
    //var cancellationTokenSource = new CancellationTokenSource();
    //var consumerTask = _consumerService.StartAsync();

    //// Publish a test message using the AsyncChannelPoolWithDataflow
    ////using (var channel = await _channelPool.GetChannelAsync())
    ////{
    ////    channel.ExchangeDeclare(_options.Exchange, _options.ExchangeType, _options.Durable);
    ////    channel.QueueDeclare(_options.Queue, _options.Durable, false, false, null);
    ////    channel.QueueBind(_options.Queue, _options.Exchange, _options.RoutingKey);

    ////    var body = Encoding.UTF8.GetBytes(message);
    ////    channel.BasicPublish(_options.Exchange, _options.RoutingKey, null, body);
    ////}
    //var messages = Enumerable.Range(0, 1000).Select(i => $"Ganesh{i}").ToList();
    //var result = await _rabbitMqLibrary.BulkPublishAsync(messages);

    //// Wait a bit to allow consumption
    //await Task.Delay(TimeSpan.FromSeconds(2000));

    //// Stop consuming messages
    //await _consumerService.StopAsync();

    //// Assert that the message was consumed
    //// Example: Assert some condition based on the message consumption
    //// You can add more assertions based on your specific requirements
    //// For example, verify if the message has been processed or logged appropriately
    //// Assert.Equal(expected, actual);

       
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
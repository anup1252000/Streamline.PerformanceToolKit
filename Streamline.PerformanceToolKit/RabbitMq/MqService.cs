using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.Text;

namespace Streamline.PerformanceToolKit.RabbitMq
{
    public class MqService : IMqService
    {
        private readonly AsyncChannelPoolWithDataflow _channelPool;
        private readonly IOptions<RabbitMqOptions> _options;
        private readonly ILogger<MqService> _logger;

        public MqService(AsyncChannelPoolWithDataflow channelPool, IOptions<RabbitMqOptions> options, ILogger<MqService> logger)
        {
            _channelPool = channelPool ?? throw new ArgumentNullException(nameof(channelPool));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            //_logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task<bool> PublishMessageAsync(string message)
        {
            return await ExecuteWithChannelAsync(async (channel, opts) =>
            {
                var body = Encoding.UTF8.GetBytes(message);
                await DeclareExchangeAndQueueAsync(channel, opts);
                channel.BasicPublish(exchange: opts.Exchange, routingKey: opts.RoutingKey, basicProperties: null, body: body);
                return true;
            });
        }

        public async Task<bool> PublishStreamMessageAsync(string message)
        {
            return await ExecuteWithChannelAsync(async (channel, opts) =>
            {
                var queueArgs = new Dictionary<string, object>
            {
                { "x-queue-type", "stream" }
            };
                channel.QueueDeclare(opts.Queue, durable: true, exclusive: false, autoDelete: false, arguments: queueArgs);
                var body = Encoding.UTF8.GetBytes(message);
                channel.BasicPublish(opts.Exchange ?? string.Empty, opts.RoutingKey, null, body);
                return true;
            });
        }

        public async Task<bool> BulkPublishAsync(IEnumerable<string> messages)
        {
            return await ExecuteWithChannelAsync(async (channel, opts) =>
            {
                channel.ConfirmSelect();
                var bulkPublish = channel.CreateBasicPublishBatch();

                var properties = channel.CreateBasicProperties();
                properties.Persistent = true;

                await DeclareExchangeAndQueueAsync(channel, opts);

                foreach (var message in messages)
                {
                    var body = Encoding.UTF8.GetBytes(message).AsMemory();
                    bulkPublish.Add(opts.Exchange, opts.RoutingKey, false, properties, body);
                }

                bulkPublish.Publish();

                if (!channel.WaitForConfirms(TimeSpan.FromSeconds(1000)))
                {
                    throw new Exception("Message confirmation failed.");
                }
                return true;
            });
        }

        private async Task<bool> ExecuteWithChannelAsync(Func<IModel, RabbitMqOptions, Task<bool>> action)
        {
            var opts = _options.Value;
            IModel channel = await _channelPool.GetChannelAsync();
            try
            {
                return await action(channel, opts);
            }
            catch (BrokerUnreachableException ex)
            {
                _logger.LogError(ex, "Broker unreachable.");
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while processing message.");
                throw;
            }
            finally
            {
                await _channelPool.ReturnChannelAsync(channel);
            }
        }

        private async Task DeclareExchangeAndQueueAsync(IModel channel, RabbitMqOptions options)
        {
            await Task.Run(() =>
            {
                channel.ExchangeDeclare(options.Exchange, options.ExchangeType, durable: options.Durable);
                channel.QueueDeclare(options.Queue, durable: options.Durable, exclusive: false, autoDelete: false, arguments: null);
                channel.QueueBind(options.Queue, options.Exchange, options.RoutingKey);
            });
        }
    }
}

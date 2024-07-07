using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Streamline.PerformanceToolKit.Common;
using System.Text;

namespace Streamline.PerformanceToolKit.RabbitMq
{
    public class RabbitMQConsumerService
    {
        private readonly IRabbitMQChannelPool _channelPool;
        private readonly IRabbitMQConsumer _consumer;
        private readonly IRetryPolicy _retryPolicy;
        private readonly ILogger<RabbitMQConsumerService> _logger;
        private readonly RabbitMqOptions _options;

        public RabbitMQConsumerService(
            IRabbitMQChannelPool channelPool,
            IRabbitMQConsumer consumer,
            IOptions<RabbitMqOptions> options,
            IRetryPolicy retryPolicy,
            ILogger<RabbitMQConsumerService> logger)
        {
            _channelPool = channelPool;
            _consumer = consumer;
            _retryPolicy = retryPolicy;
            _logger = logger;
            _options = options.Value;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            var channel = await _channelPool.GetChannelAsync();
            var consumer = new AsyncEventingBasicConsumer(channel);
            var buffer = new List<BasicDeliverEventArgs>();

            consumer.Received += async (model, ea) =>
            {
                buffer.Add(ea);

                if (buffer.Count >= _options.BatchSize)
                {
                    await HandleMessagesAsync(buffer.ToArray(), cancellationToken);
                    buffer.Clear();
                }
            };

            channel.BasicConsume(queue: _options.Queue, autoAck: false, consumer: consumer);
        }

        private async Task HandleMessagesAsync(BasicDeliverEventArgs[] messages, CancellationToken cancellationToken)
        {
            try
            {
                await _retryPolicy.ExecuteAsync(async () =>
                {
                    await _consumer.ProcessMessagesAsync(messages, cancellationToken);
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process messages.");
                foreach (var message in messages)
                {
                    await HandleFallbackAsync(message);
                }
            }
        }

        private async Task HandleFallbackAsync(BasicDeliverEventArgs ea)
        {
            var channel = await _channelPool.GetChannelAsync();
            try
            {
                if (_options.UseDeadLetterExchange)
                {
                    channel.BasicPublish(
                        exchange: _options.DeadLetterExchangeName,
                        routingKey: _options.DeadLetterRoutingKey,
                        basicProperties: ea.BasicProperties,
                        body: ea.Body);
                }
                else
                {
                    _logger.LogError("Message processing failed: {Body}", Encoding.UTF8.GetString(ea.Body.ToArray()));
                }
                channel.BasicAck(ea.DeliveryTag, false);
            }
            finally
            {
                await _channelPool.ReturnChannelAsync(channel);
            }
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Stopping RabbitMQ Consumer Service.");
            await _channelPool.DisposeAsync();
        }
    }

}

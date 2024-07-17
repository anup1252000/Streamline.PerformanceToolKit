using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Streamline.PerformanceToolKit.Common;
using System.Text;

namespace Streamline.PerformanceToolKit.RabbitMq
{
    public class RabbitMQConsumerService(
        IRabbitMQChannelPool channelPool,
        IRabbitMQConsumer consumer,
        IOptions<RabbitMqOptions> options,
        IRetryPolicy retryPolicy,
        ILogger<RabbitMQConsumerService> logger)
    {
        private readonly IRabbitMQChannelPool _channelPool = channelPool;
        private readonly IRabbitMQConsumer _consumer = consumer;
        private readonly IRetryPolicy _retryPolicy = retryPolicy;
        private readonly ILogger<RabbitMQConsumerService> _logger = logger;
        private readonly RabbitMqOptions _options = options.Value;
        private readonly CancellationTokenSource _cancellationTokenSource = new();

        public async Task StartAsync()
        {
            var cancellationToken = _cancellationTokenSource.Token;

            try
            {
                var channel = await _channelPool.GetChannelAsync();
                try
                {
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1000, global: false);

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += async (sender, eventArgs) =>
                    {
                        var message = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
                       // _logger.LogInformation("Received message: {Message}", message);

                        // Process the message
                        await ProcessMessageAsync(eventArgs);

                        // Acknowledge the message
                        channel.BasicAck(eventArgs.DeliveryTag, multiple: false);
                    };

                    channel.BasicConsume(
                        queue: _options.Queue,
                        autoAck: false,
                        consumer: consumer);

                   // _logger.LogInformation("Consumer started. Listening to queue: {Queue}", _options.Queue);

                    // Wait until cancellation is requested
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Consumer stopped due to cancellation request.");
                }
                finally
                {
                    await _channelPool.ReturnChannelAsync(channel);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start consuming messages.");
            }
        }

        public async Task StopAsync()
        {
            _cancellationTokenSource.Cancel();
            await _channelPool.DisposeAsync();
            _logger.LogInformation("RabbitMQ Consumer Service stopped.");
        }

        private async Task ProcessMessageAsync(BasicDeliverEventArgs eventArgs)
        {
            try
            {
                await _retryPolicy.ExecuteAsync(async () =>
                {
                    await _consumer.ProcessMessagesAsync([eventArgs], CancellationToken.None);
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process message, requeuing.");
                await HandleFallbackAsync(eventArgs);
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
    }
}

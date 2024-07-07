using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System.Diagnostics;
using System.Text;

namespace Streamline.PerformanceToolKit.RabbitMq
{
    public class RabbitMQConsumer : IRabbitMQConsumer
    {
        private readonly IOptions<RabbitMqOptions> _options;
        private readonly AsyncChannelPoolWithDataflow _channelPool;
        private readonly ILogger<RabbitMQConsumer> _logger;

        public RabbitMQConsumer(IOptions<RabbitMqOptions> options, AsyncChannelPoolWithDataflow channelPool, ILogger<RabbitMQConsumer> logger)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _channelPool = channelPool ?? throw new ArgumentNullException(nameof(channelPool));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public Task ProcessMessagesAsync(IEnumerable<BasicDeliverEventArgs> messages, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public void StartConsuming()
        {
            Task.Run(async () =>
            {
                 var channel = await _channelPool.GetChannelAsync();
                try
                {
                    var consumer = new AsyncEventingBasicConsumer(channel);
                    consumer.Received += async (sender, eventArgs) =>
                    {
                        var body = eventArgs.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);

                        _logger.LogInformation($"Received message: {message}");

                        // Simulate message processing (replace with your actual logic)
                        await SimulateMessageProcessing();

                        channel.BasicAck(eventArgs.DeliveryTag, false);
                    };

                    channel.BasicConsume(queue: _options.Value.Queue,
                                         autoAck: false,
                                         consumer: consumer);

                    await Task.Delay(Timeout.Infinite); // Wait indefinitely (or implement your own start/stop mechanism)
                }
                catch (OperationInterruptedException ex)
                {
                    _logger.LogError(ex, "Operation interrupted in RabbitMQ consumer.");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in RabbitMQ consumer.");
                }
                finally
                {
                    await _channelPool.ReturnChannelAsync(channel);
                }
            });
        }

        private async Task SimulateMessageProcessing()
        {
            // Simulate processing delay
            await Task.Delay(TimeSpan.FromSeconds(1));
        }
    }

}

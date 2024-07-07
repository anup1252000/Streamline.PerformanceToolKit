using RabbitMQ.Client.Events;

namespace Streamline.PerformanceToolKit.RabbitMq
{
    public interface IRabbitMQConsumer
    {
        Task ProcessMessagesAsync(IEnumerable<BasicDeliverEventArgs> messages, CancellationToken cancellationToken);
    }
}

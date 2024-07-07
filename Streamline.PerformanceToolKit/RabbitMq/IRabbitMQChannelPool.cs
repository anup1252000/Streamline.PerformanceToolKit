using RabbitMQ.Client;

namespace Streamline.PerformanceToolKit.RabbitMq
{
    public interface IRabbitMQChannelPool : IAsyncDisposable
    {
        Task<IModel> GetChannelAsync();
        Task ReturnChannelAsync(IModel channel);
    }
}
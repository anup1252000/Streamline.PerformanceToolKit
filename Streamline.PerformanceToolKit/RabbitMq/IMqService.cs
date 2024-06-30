namespace Streamline.PerformanceToolKit.RabbitMq
{
    public interface IMqService
    {
        Task<bool> PublishMessageAsync(string message, RabbitMqOptions options);
        Task<bool> PublishStreamMessage(string message, RabbitMqOptions options);
        Task<bool> BulkPublishAsync(IEnumerable<string> messages, RabbitMqOptions options);
    }
}

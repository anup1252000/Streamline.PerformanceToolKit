namespace Streamline.PerformanceToolKit.RabbitMq
{
    public interface IMqService
    {
        Task<bool> PublishMessageAsync(string message);
        Task<bool> PublishStreamMessageAsync(string message);
        Task<bool> BulkPublishAsync(IEnumerable<string> messages);
    }
}

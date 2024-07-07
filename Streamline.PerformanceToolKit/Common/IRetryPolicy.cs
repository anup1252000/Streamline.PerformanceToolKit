namespace Streamline.PerformanceToolKit.Common
{
    public interface IRetryPolicy
    {
        Task ExecuteAsync(Func<Task> action);
    }
}

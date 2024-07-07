using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Streamline.PerformanceToolKit.RabbitMq;

namespace Streamline.PerformanceToolKit.Common
{
    public class PollyRetryPolicy : IRetryPolicy
    {
        private readonly ILogger<PollyRetryPolicy> _logger;
        private readonly IAsyncPolicy _retryPolicy;

        public PollyRetryPolicy(IOptions<RabbitMqOptions> options, ILogger<PollyRetryPolicy> logger)
        {
            _logger = logger;
            var retryCount = options.Value.RetryCount;
            var retryDelay = options.Value.RetryDelayMilliseconds;

            _retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(
                    retryCount,
                    retryAttempt => TimeSpan.FromMilliseconds(retryDelay),
                    (exception, timeSpan, retryCount, context) =>
                    {
                        _logger.LogWarning(exception, "Retry {RetryCount} encountered an error. Waiting {Delay}ms", retryCount, timeSpan.TotalMilliseconds);
                    });
        }

        public async Task ExecuteAsync(Func<Task> action)
        {
            await _retryPolicy.ExecuteAsync(action);
        }
    }

}

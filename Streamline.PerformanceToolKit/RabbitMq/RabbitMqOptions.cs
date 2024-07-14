using System.Security.AccessControl;

namespace Streamline.PerformanceToolKit.RabbitMq
{
    public class RabbitMqOptions
    {
        public string UserName { get; set; }
        public string Password { get; set; }
        public string Host { get; set; }
        public string Exchange { get; set; }
        public string RoutingKey { get; set; }
        public string Queue { get; set; }
        public bool Durable { get; set; }
        public string ExchangeType { get; set; }
        public int BatchSize { get; set; } = 10;
        public ushort PrefetchCount { get; set; } = 10;
        public string VirtualHost { get; set; } = "/";
        public int Port { get; set; } = 5672;
        public int BatchTimeout { get; set; } = 2000;
        public int MaxDegreeOfParallelism { get; set; } = 4;
        public int BoundedCapacity { get; set; } = 100;
        public int RetryCount { get; set; } = 3;
        public int RetryDelayMilliseconds { get; set; } = 500;
        public bool UseDeadLetterExchange { get; set; } = false; // New
        public string DeadLetterExchangeName { get; set; }
        public string DeadLetterQueueName { get; set; }
        public string DeadLetterRoutingKey { get; set; }
        public int ChannelPoolSize { get; set; } = 5; // Default pool size
        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(Host)) throw new ArgumentException("HostName must be provided.");
            if (string.IsNullOrWhiteSpace(Queue)) throw new ArgumentException("QueueName must be provided.");
            if (BatchSize <= 0) throw new ArgumentException("BatchSize must be greater than zero.");
            if (PrefetchCount <= 0) throw new ArgumentException("PrefetchCount must be greater than zero.");
            if (string.IsNullOrWhiteSpace(UserName)) throw new ArgumentException("UserName must be provided.");
            if (string.IsNullOrWhiteSpace(Password)) throw new ArgumentException("Password must be provided.");
            if (Port <= 0) throw new ArgumentException("Port must be a positive number.");
            if (MaxDegreeOfParallelism <= 0) throw new ArgumentException("MaxDegreeOfParallelism must be greater than zero.");
            if (BoundedCapacity <= 0) throw new ArgumentException("BoundedCapacity must be greater than zero.");
            if (RetryCount < 0) throw new ArgumentException("RetryCount must be zero or greater.");
            if (RetryDelayMilliseconds < 0) throw new ArgumentException("RetryDelayMilliseconds must be zero or greater.");
            if (UseDeadLetterExchange)
            {
                if (string.IsNullOrWhiteSpace(DeadLetterExchangeName)) throw new ArgumentException("DeadLetterExchangeName must be provided.");
                if (string.IsNullOrWhiteSpace(DeadLetterQueueName)) throw new ArgumentException("DeadLetterQueueName must be provided.");
                if (string.IsNullOrWhiteSpace(DeadLetterRoutingKey)) throw new ArgumentException("DeadLetterRoutingKey must be provided.");
            }
        }
    }

}

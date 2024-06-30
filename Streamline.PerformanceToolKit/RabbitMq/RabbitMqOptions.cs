namespace Streamline.PerformanceToolKit.RabbitMq
{
    public record RabbitMqOptions
    {
        public string UserName { get; set; }
        public string Password { get; set; }
        public int Port { get; set; }
        public string Host { get; set; }
        public string Exchange { get; set; }
        public string RoutingKey { get; set; }
        public string Queue { get; set; }
        public bool Durable { get; set; }
        public string ExchangeType { get; set; }
    }

}

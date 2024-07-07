using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Streamline.PerformanceToolKit.Common;


namespace Streamline.PerformanceToolKit.RabbitMq
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddRabbitMQLibrary(this IServiceCollection services, IConfiguration configuration)
        {
            // Register options
            services.Configure<RabbitMqOptions>(x=>configuration.GetSection("RabbitMQ"));

            // Register AsyncChannelPoolWithDataflow as a singleton
            services.AddSingleton<IRabbitMQChannelPool, AsyncChannelPoolWithDataflow>();

            // Register consumer and related services as transient
            services.AddTransient<IRabbitMQConsumer, RabbitMQConsumer>();
            services.AddTransient<IRetryPolicy, PollyRetryPolicy>();

            return services;
        }
    }
}

using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.Text;

namespace Streamline.PerformanceToolKit.RabbitMq
{
    public class MqService : IMqService
    {
        private readonly AsyncChannelPoolWithDataflow asyncChannel;
        private readonly IOptions<RabbitMqOptions> rabbitOptions;

        public MqService(AsyncChannelPoolWithDataflow asyncChannel,IOptions<RabbitMqOptions> options)
        {
            this.asyncChannel = asyncChannel;
            this.rabbitOptions = options;
        }
        public async Task<bool> PublishMessageAsync(string message, RabbitMqOptions options)
        {
            try
            {

                IModel channel = await asyncChannel.GetChannelAsync();
                try
                {
                    await DeclareExchangeAndQueueAsync(channel, options);
                    var body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(exchange: options.Exchange, routingKey: options.RoutingKey, basicProperties: null, body: body);
                    return true;
                }
                finally 
                {
                    await asyncChannel.ReturnChannelAsync(channel);
                }

                #region Commented
                //var factory = new ConnectionFactory
                //{
                //    HostName = options.Host,
                //    UserName = options.UserName,
                //    Password = options.Password,
                //    Port = options.Port
                //};

                //using (var connection = factory.CreateConnection())
                //using (var channel = connection.CreateModel())
                //{
                //    await DeclareExchangeAndQueueAsync(channel, options);
                //    var body = Encoding.UTF8.GetBytes(message);
                //    channel.BasicPublish(exchange: options.Exchange, routingKey: options.RoutingKey, basicProperties: null, body: body);
                //    return true;
                //}
                #endregion
            }
            catch (BrokerUnreachableException)
            {
                throw;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async  Task<bool> PublishStreamMessage(string message, RabbitMqOptions options)
        {
            try
            {

                IModel channel = await asyncChannel.GetChannelAsync();
                try
                {
                    var queueArg = new Dictionary<string, object>
                    {
                          { "x-queue-type", "stream" }
                    };
                    channel.QueueDeclare(options.Queue, durable: true, exclusive: false, autoDelete: false, arguments: queueArg);
                    var body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(options.Exchange ?? string.Empty, options.RoutingKey, null, body);
                    return true;
                }
                finally
                {
                    await asyncChannel.ReturnChannelAsync(channel);
                }

                #region Commented
                // var factory = new ConnectionFactory
                // {
                //     HostName = options.Host,
                //     UserName = options.UserName,
                //     Password = options.Password,
                //     Port = options.Port
                // };

                // using (var connection = factory.CreateConnection())
                // using (var channel = connection.CreateModel())
                // {
                //     var queueArg = new Dictionary<string, object>
                //{
                //      { "x-queue-type", "stream" }
                //};
                //     channel.QueueDeclare(options.Queue, durable: true, exclusive: false, autoDelete: false, arguments: queueArg);
                //     var body = Encoding.UTF8.GetBytes(message);
                //     channel.BasicPublish(options.Exchange ?? string.Empty, options.RoutingKey, null, body);
                // }
                // return true;

                #endregion
            }
            catch (BrokerUnreachableException)
            {
                throw;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<bool> BulkPublishAsync(IEnumerable<string> messages,RabbitMqOptions options)
        {
            try
            {
                IModel channel = await asyncChannel.GetChannelAsync();
                try
                {
                    channel.ConfirmSelect();
                    var bulkPublish = channel.CreateBasicPublishBatch();

                    channel.QueueDeclare(options.Queue, durable: true, exclusive: false, autoDelete: false);
                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;

                    foreach (var message in messages)
                    {
                        var body = Encoding.UTF8.GetBytes(message).AsMemory();
                        bulkPublish.Add(options.Exchange, options.RoutingKey, false, properties, body);
                    }

                    bulkPublish.Publish();

                    // Manually wait for confirms
                    foreach (var message in messages)
                    {
                        if (!channel.WaitForConfirms(TimeSpan.FromSeconds(10)))
                        {
                            // Handle failed confirmation
                            Console.WriteLine("Failed to confirm message: " + message);
                        }
                    }
                    return true;
                }
                finally
                {
                    await asyncChannel.ReturnChannelAsync(channel);
                }

                #region Commented
                //var factory = new ConnectionFactory
                //{
                //    HostName = options.Host,
                //    UserName = options.UserName,
                //    Password = options.Password,
                //    Port = options.Port
                //};
                //using (var connection = factory.CreateConnection())
                //using (var channel = connection.CreateModel())
                //{
                //    channel.ConfirmSelect();
                //    var bulkPublish = channel.CreateBasicPublishBatch();

                //    channel.QueueDeclare(options.Queue, durable: true, exclusive: false, autoDelete: false);
                //    var properties = channel.CreateBasicProperties();
                //    properties.Persistent = true;
                //    foreach (var message in messages)
                //    {
                //        var body = Encoding.UTF8.GetBytes(message).AsMemory();
                //        bulkPublish.Add(options.Exchange, options.RoutingKey, false, properties, body);
                //    }
                //    bulkPublish.Publish();
                //}


                //return true;
                #endregion
            }
            catch (BrokerUnreachableException)
            {
                throw;
            }
            catch (Exception)
            {
                throw;
            }

        }

        private async Task DeclareExchangeAndQueueAsync(IModel channel, RabbitMqOptions options)
        {
            await Task.Run(() =>
            {
                channel.ExchangeDeclare(options.Exchange, options.ExchangeType, durable: options.Durable);
                channel.QueueDeclare(options.Queue, durable: options.Durable, exclusive: false, autoDelete: false, arguments: null);
                channel.QueueBind(options.Queue, options.Exchange, options.RoutingKey);
            });
        }
    }
}

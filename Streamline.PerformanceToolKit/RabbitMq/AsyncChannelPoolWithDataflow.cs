using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System.Threading.Tasks.Dataflow;

namespace Streamline.PerformanceToolKit.RabbitMq
{
    public class AsyncChannelPoolWithDataflow : IRabbitMQChannelPool
    {
        private readonly IOptions<RabbitMqOptions> _options;
        private IConnection _connection;
        private readonly SemaphoreSlim _connectionLock = new(1, 1);
        private bool _disposed;

        private readonly TransformBlock<int, IModel> _channelCreationBlock;
        private readonly BufferBlock<IModel> _channelBufferBlock;

        public AsyncChannelPoolWithDataflow(IOptions<RabbitMqOptions> options)
        {
            _options = options;
            var poolSize = options.Value.ChannelPoolSize;

            _channelBufferBlock = new BufferBlock<IModel>(new DataflowBlockOptions
            {
                BoundedCapacity = poolSize
            });

            _channelCreationBlock = new TransformBlock<int, IModel>(async _ =>
            {
                return await CreateChannelAsync();
            }, new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = DataflowBlockOptions.Unbounded,
                BoundedCapacity = poolSize
            });

            _channelCreationBlock.LinkTo(_channelBufferBlock, new DataflowLinkOptions { PropagateCompletion = true });

            InitializePoolAsync(poolSize).GetAwaiter().GetResult();
            StartConnectionHealthCheck();
        }

        private async Task InitializePoolAsync(int poolSize)
        {
            for (int i = 0; i < poolSize; i++)
            {
                _channelBufferBlock.Post(await CreateChannelAsync());
            }
        }
        private async Task<IConnection> GetOrCreateConnectionAsync()
        {
            if (_connection == null || !_connection.IsOpen)
            {
                await _connectionLock.WaitAsync();
                try
                {
                    if (_connection == null || !_connection.IsOpen)
                    {
                        _connection?.Dispose();
                        _connection = await Task.Run(CreateConnection);
                    }
                }
                finally
                {
                    _connectionLock.Release();
                }
            }
            return _connection;
        }

        private IConnection CreateConnection()
        {
            var factory = new ConnectionFactory
            {
                HostName = _options.Value.Host,
                UserName = _options.Value.UserName,
                Password = _options.Value.Password,
                AutomaticRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
                TopologyRecoveryEnabled = true
            };

            return factory.CreateConnection();
        }

        private async Task<IModel> CreateChannelAsync()
        {
            var connection = await GetOrCreateConnectionAsync();
            return await Task.Run(() => connection.CreateModel());
        }

        public async Task<IModel> GetChannelAsync()
        {
            if (_channelBufferBlock.TryReceive(out var channel))
            {
                if (channel.IsOpen)
                {
                    return channel;
                }
                else
                {
                    channel.Dispose();
                    return await CreateChannelAsync();
                }
            }
            else
            {
                var available = await _channelBufferBlock.OutputAvailableAsync();
                if (available)
                {
                    return await _channelBufferBlock.ReceiveAsync();
                }
                else
                {
                    return await CreateChannelAsync();
                }
            }
        }

        public async Task ReturnChannelAsync(IModel channel)
        {
            if (channel.IsOpen)
            {
                await _channelBufferBlock.SendAsync(channel);
            }
            else
            {
                channel.Dispose();
                if (_channelBufferBlock.Count < _options.Value.ChannelPoolSize)
                {
                    await _channelBufferBlock.SendAsync(await CreateChannelAsync());
                }
            }
        }

        private void StartConnectionHealthCheck()
        {
            _ = Task.Run(async () =>
            {
                while (!_disposed)
                {
                    await Task.Delay(TimeSpan.FromMinutes(1));
                    if (_connection != null && !_connection.IsOpen)
                    {
                        await _connectionLock.WaitAsync();
                        try
                        {
                            _connection?.Dispose();
                            _connection = await Task.Run(CreateConnection);
                        }
                        finally
                        {
                            _connectionLock.Release();
                        }
                    }
                }
            });
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            while (_channelBufferBlock.TryReceive(out var channel))
            {
                channel.Dispose();
            }
            _connection?.Dispose();
            _disposed = true;

            _channelCreationBlock.Complete();
            await _channelCreationBlock.Completion;

            _channelBufferBlock.Complete();
            await _channelBufferBlock.Completion;
        }
    }
}

using CommandLine;
using CommandLine.Text;
using Microsoft.Extensions.ObjectPool;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Runtime;
using System.Text;

class Program
{
    [Option('c', "concurrency", Required = false)]
    public int Concurrency { get; set; } = 10;
    [Option('p', "prefetchCount", Required = false)]
    public ushort PrefetchCount { get; set; } = 100;
    [Option('r', "rateCount", Required = false)]
    public int RateCount { get; set; } = 1;
    [Option('s', "rateDuration", Required = false)]
    public int RateDuration { get; set; } = 1000;
    [Option('q', "queueName", Required = false)]
    public string? QueueName { get; set; }
    [Option('d', "durable", Required = false, Default = true)]
    public bool Durable { get; set; } = true;
    [Option('l', "latencyMode", Required = false)]
    public GCLatencyMode LatencyMode { get; set; } = GCSettings.LatencyMode;

    [Option("err", Required = false)]
    public int ErrorThresshold { get; set; } = int.MaxValue;
    [Option("wrn", Required = false)]
    public int WarnThresshold { get; set; } = int.MaxValue;
    [Option("inf", Required = false)]
    public int InfoThresshold { get; set; } = int.MaxValue;
    [Option("dbg", Required = false)]
    public int DebugThresshold { get; set; } = int.MaxValue;

    ConcurrentDictionary<ulong, TaskCompletionSource<bool>> messages = new();
    long received = 0;
    long latencySum = 0;
    Stopwatch started = Stopwatch.StartNew();

    static Task Main(string[] args)
    {
        return Parser.Default.ParseArguments<Program>(args).WithParsedAsync(o => o.Run());
    }

    async Task Run()
    {
        if (QueueName == null) QueueName = Durable ? "durable" : "non-durable";

        foreach (PropertyDescriptor descriptor in TypeDescriptor.GetProperties(this))
        {
            string name = descriptor.Name;
            object? value = descriptor.GetValue(this);
            Console.WriteLine("\t{0} = {1}", name, value);
        }

        AppDomain.CurrentDomain.FirstChanceException += (s, ea) => Console.WriteLine(ea.Exception);

        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (s, ea) =>
        {
            ea.Cancel = true;
            cts.Cancel();
        };

        const string EnvVar = "RABBITMQ_URI";

        var cs = Environment.GetEnvironmentVariable(EnvVar);
        if (cs == null) throw new InvalidOperationException($"No envvar {EnvVar} exist.");

        var factory = new ConnectionFactory()
        {
            Uri = new Uri(cs),
            DispatchConsumersAsync = true,
            ConsumerDispatchConcurrency = Concurrency,
            AutomaticRecoveryEnabled = true
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();
        channel.QueueDeclare(queue: QueueName,
                                            durable: Durable,
                                            exclusive: false,
                                            autoDelete: false,
                                            arguments: null
                                            );
        channel.BasicQos(0, PrefetchCount, false);
        channel.BasicAcks += Channel_BasicAcks;
        channel.BasicNacks += Channel_BasicNacks;
        channel.ModelShutdown += Channel_ModelShutdown;
        channel.BasicReturn += Channel_BasicReturn;
        channel.ConfirmSelect();

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += Channel_Received;

        channel.BasicConsume(
            queue: QueueName,
            autoAck: false,
            consumer: consumer
            );


        string message = "Hello World!";
        var body = Encoding.UTF8.GetBytes(message);
        const string ContentType = "text/plain; charset=utf-8";

        var pool = new DefaultObjectPool<Dictionary<string, object>>(new DefaultPooledObjectPolicy<Dictionary<string, object>>());

        GCSettings.LatencyMode = LatencyMode;
        WriteLine($"LatencyMode = {GCSettings.LatencyMode}");
        var gate = new RateGate(RateCount, TimeSpan.FromMilliseconds(RateDuration));

        Report(cts.Token);

        while (!cts.IsCancellationRequested)
        {
            await gate.WaitAsync(-1, cts.Token);

            _ = Task.Run(async () =>
            {
                var headers = pool.Get();
                try
                {
                    var now = DateTime.UtcNow;
                    var props = channel.CreateBasicProperties();
                    props.ContentType = ContentType;
                    props.Persistent = Durable;
                    props.Headers = headers;
                    props.Expiration = "15000";

                    var sentAt = now.ToString("O");
                    headers["at"] = sentAt;

                    var sw = Stopwatch.StartNew();
                    TaskCompletionSource<bool> tcs;
                    lock (channel)
                    {
                        tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                        var added = messages.TryAdd(channel.NextPublishSeqNo, tcs);
                        if (!added) throw new Exception($"Cannot publish a message with sequence number '{channel.NextPublishSeqNo}' on this channel. A message was already published on this channel with the same confirmation number.");

                        props.SetConfirmationId(channel.NextPublishSeqNo);

                        channel.BasicPublish(exchange: string.Empty,
                                                routingKey: QueueName,
                                                basicProperties: props,
                                                body: body);
                    }
                    await tcs.Task;
                    var duration = sw.ElapsedMilliseconds;
                    //WriteLine($"[DBG]                                                             Sent at {sentAt,12:O} duration: {duration,8:N1}ms", ConsoleColor.DarkGreen);
                }
                finally
                {
                    pool.Return(headers);
                }
            });
        }
    }

    async Task Channel_Received(object s, BasicDeliverEventArgs ea)
    {
        await Task.Yield();
        var nr = Interlocked.Increment(ref received);
        var ackChannel = ((AsyncDefaultBasicConsumer)s).Model;
        try
        {
            var now = DateTime.UtcNow;
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            var data = (byte[])ea.BasicProperties.Headers["at"];
            var at = DateTime.Parse(Encoding.UTF8.GetString(data), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            var latency = now - at;

            Interlocked.Add(ref latencySum, (long)latency.TotalMilliseconds);

            if (latency.TotalMilliseconds > ErrorThresshold)
            {
                WriteLine($"[ERR] #{nr,5} {at,12:O} {latency.TotalMilliseconds,8:N1}ms", ConsoleColor.Red);
            }
            else if (latency.TotalMilliseconds > WarnThresshold)
            {
                WriteLine($"[WRN] #{nr,5} {at,12:O} {latency.TotalMilliseconds,8:N1}ms", ConsoleColor.DarkYellow);
            }
            else if (latency.TotalMilliseconds > InfoThresshold)
            {
                WriteLine($"[INF] #{nr,5} {at,12:O} {latency.TotalMilliseconds,8:N1}ms");
            }
            else if (latency.TotalMilliseconds > DebugThresshold)
            {
                WriteLine($"[DBG] #{nr,5} {at,12:O} {latency.TotalMilliseconds,8:N1}ms", ConsoleColor.DarkGreen);
            }

            ackChannel.BasicAck(ea.DeliveryTag, false);
        }
        catch (Exception e)
        {
            ackChannel.BasicReject(ea.DeliveryTag, requeue: false);
            WriteLine(e.ToString(), ConsoleColor.Magenta);
        }
    }

    void Channel_BasicNacks(object? sender, BasicNackEventArgs e)
    {
        if (!e.Multiple)
        {
            SetException(e.DeliveryTag, "Message rejected by broker.");
        }
        else
        {
            foreach (var message in messages)
            {
                if (message.Key <= e.DeliveryTag)
                {
                    SetException(message.Key, "Message rejected by broker.");
                }
            }
        }
    }

    void Channel_BasicAcks(object? sender, BasicAckEventArgs e)
    {
        if (!e.Multiple)
        {
            SetResult(e.DeliveryTag);
        }
        else
        {
            foreach (var message in messages)
            {
                if (message.Key <= e.DeliveryTag)
                {
                    SetResult(message.Key);
                }
            }
        }
    }

    void Channel_BasicReturn(object? sender, BasicReturnEventArgs e)
    {
        var message = $"Message could not be routed to {e.Exchange + e.RoutingKey}: {e.ReplyCode} {e.ReplyText}";
        SetException(e.BasicProperties.GetConfirmationId(), message);
    }

    void Channel_ModelShutdown(object? sender, ShutdownEventArgs e)
    {
        do
        {
            foreach (var message in messages)
            {
                SetException(message.Key, $"Channel has been closed: {e}");
            }
        }
        while (!messages.IsEmpty);
    }

    void SetException(ulong key, string exceptionMessage)
    {
        if (messages.TryRemove(key, out var tcs))
        {
            tcs.SetException(new Exception(exceptionMessage));
        }
    }

    void SetResult(ulong key)
    {
        if (messages.TryRemove(key, out var tcs))
        {
            tcs.SetResult(true);
        }
    }

    void WriteLine(string message, ConsoleColor color = ConsoleColor.Gray)
    {
        _ = Task.Run(() =>
        {
            lock (Console.Out)
            {
                if (color == ConsoleColor.Gray)
                {
                    Console.Out.WriteLine(message);

                }
                else
                {
                    Console.ForegroundColor = color;
                    Console.Out.WriteLine(message);
                    Console.ForegroundColor = ConsoleColor.Gray;
                }
            }
        });
    }

    async void Report(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var elapsed = started.Elapsed;
            await Console.Out.WriteLineAsync($"received:{received,10:N0} elapsed:{elapsed.TotalSeconds,10:N0}s throughput:{received / elapsed.TotalSeconds,10:N0}msg/s latency:{(received > 0 ? (latencySum / received) : -1)}ms");
            await Task.Delay(TimeSpan.FromSeconds(10));
        }
    }
}

static class BasicPropertiesConfirmationExtensions
{
    public static void SetConfirmationId(this IBasicProperties properties, ulong nextPublishSeqNod)
    {
        properties.Headers[NextPublishSeqNodHeader] = nextPublishSeqNod.ToString();
    }

    public static ulong GetConfirmationId(this IBasicProperties properties)
    {

        return ulong.Parse(Encoding.UTF8.GetString((byte[])properties.Headers[NextPublishSeqNodHeader]));
    }

    public const string NextPublishSeqNodHeader = "NextPublishSeqNo";
}

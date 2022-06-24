using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text;

int Concurrency = 10;
ushort PrefetchCount = 100;

const string QueueName = "native";
const bool Durable = true;

static DateTime RoundUp(DateTime dt, TimeSpan d) => new DateTime((dt.Ticks + d.Ticks - 1) / d.Ticks * d.Ticks, dt.Kind);

var messages = new ConcurrentDictionary<ulong, TaskCompletionSource<bool>>();

AppDomain.CurrentDomain.FirstChanceException += (s, ea) => Console.WriteLine(ea.Exception);

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (s, ea) =>
{
    ea.Cancel = true;
    cts.Cancel();
};
Console.WriteLine(" Press Ctrl+C to exit.");

var factory = new ConnectionFactory()
{
    Uri = new Uri(Environment.GetEnvironmentVariable("RABBITMQ_CONNECTIONSTRING")),
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

void WriteLine(string message)
{
    _ = Console.Out.WriteLineAsync(message);
}

long received = 0;

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.Received += async (s, ea) =>
{
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

        WriteLine($"#{nr,5} {at,12:O} {latency.TotalMilliseconds,8:N1}ms");
        ackChannel.BasicAck(ea.DeliveryTag, false);
    }
    catch (global::System.Exception e)
    {
        ackChannel.BasicReject(ea.DeliveryTag, requeue: false);
        WriteLine(e.ToString());
    }
};


channel.BasicConsume(
    queue: QueueName,
    autoAck: false,
    consumer: consumer
    );

var step = TimeSpan.FromMilliseconds(50);

var now = DateTime.UtcNow;
now = RoundUp(now, step);
var next = now + step;

string message = "Hello World!";
var body = Encoding.UTF8.GetBytes(message);

while (!cts.IsCancellationRequested)
{
    var delay = next - DateTime.UtcNow;

    if (delay.Ticks > 0) await Task.Delay(delay);

    now = DateTime.UtcNow;
    next += step;

    _ = Task.Run(async () =>
    {
        var props = channel.CreateBasicProperties();
        props.ContentType = "text/plain; charset=utf-8";
        props.DeliveryMode = 2;

        var headers = props.Headers = new Dictionary<string, object>();

        var sentAt = now.ToString("O");
        headers.Add("at", sentAt);

        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var added = messages.TryAdd(channel.NextPublishSeqNo, tcs);
        if (!added) throw new Exception($"Cannot publish a message with sequence number '{channel.NextPublishSeqNo}' on this channel. A message was already published on this channel with the same confirmation number.");

        props.SetConfirmationId(channel.NextPublishSeqNo);

        channel.BasicPublish(exchange: string.Empty,
                                routingKey: QueueName,
                                basicProperties: props,
                                body: body);
        await tcs.Task;
        //Console.WriteLine(" [x] Sent {0} {1}", message, sentAt);
    });
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
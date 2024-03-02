using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using System.Text.Json;

namespace ServiceBusUtil;
internal class ServiceBusProcessor
{
    public static async Task ListTopicsAndSubscriptionsAsync(string connectionString)
    {
        var serviceBusAdminClient = new ServiceBusAdministrationClient(connectionString);
        var topics = serviceBusAdminClient.GetTopicsAsync();
        await foreach (var topic in topics)
        {
            var subs = serviceBusAdminClient.GetSubscriptionsAsync(topic.Name);
            await foreach (var sub in subs)
            {
                Console.WriteLine($"{sub.TopicName}: {sub.SubscriptionName}");
            }
        }
    }

    public static async Task PurgeDeadLetterQueueAsync(string connectionString, string topic, string subscriber)
    {
        Console.WriteLine($"Starting DLQ purge for {topic}/{subscriber}");

        var serviceBusClient = new ServiceBusClient(connectionString);
        var receiverOptions = new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter, ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete };
        var receiver = serviceBusClient.CreateReceiver(topic, subscriber, receiverOptions);

        var hasProcessed = true;
        long messagesProcessed = 0;

        while (hasProcessed)
        {
            var messages = await receiver.ReceiveMessagesAsync(100, null, CancellationToken.None);
            if (messages.Count == 0)
            {
                hasProcessed = false;
            }

            messagesProcessed += messages.Count;
        }

        Console.WriteLine($"Processed {messagesProcessed} records");
    }

    public static async Task PurgeDeadLetterQueueOlderThanOneMonth(string connectionString, string topic, string subscriber)
    {

        var serviceBusClient = new ServiceBusClient(connectionString);
        var receiverOptions = new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter };
        var receiver = serviceBusClient.CreateReceiver(topic, subscriber, receiverOptions);

        await PurgeAllAsync(receiver);
    }

    public static async Task PurgeQueueAsync(string connectionString, string queueName)
    {

        var serviceBusClient = new ServiceBusClient(connectionString);
        var receiverOptions = new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete };
        var receiver = serviceBusClient.CreateReceiver(queueName, receiverOptions);

        await PurgeAllAsync(receiver);
    }

    private static async Task PurgeAllAsync(ServiceBusReceiver receiver)
    {
        var hasProcessed = true;
        while (hasProcessed)
        {
            var msg = await receiver.ReceiveMessageAsync();
            if (msg == null)
            {
                hasProcessed = false;
                continue;
            }
            await receiver.CompleteMessageAsync(msg);
        }
    }
    
    public static async Task SendAsync(TestEvent @event, string connectionString, string topicName)
    {
        var client = new ServiceBusClient(connectionString);
        var queueClient = client.CreateSender(topicName);

        var serviceBusMessage = new ServiceBusMessage(JsonSerializer.Serialize(@event))
        {
            MessageId = @event.UniqueId.ToString()
        };

        await queueClient.SendMessageAsync(serviceBusMessage);
    }

    public static async Task SendBatchAsync(IEnumerable<TestEvent> events, string connectionString, string topicName)
    {
        var client = new ServiceBusClient(connectionString);
        var queueClient = client.CreateSender(topicName);
        var messagesToSend = events.Select(@event => new ServiceBusMessage(JsonSerializer.Serialize(@event))).ToList();

        await queueClient.SendMessagesAsync(messagesToSend);
    }
}
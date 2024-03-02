using Microsoft.Extensions.Configuration;
using System.CommandLine;
using System.CommandLine.Invocation;

namespace ServiceBusUtil;

internal class Program
{
    private static async Task Main(string[] args)
    {
        var config = new ConfigurationBuilder()
            .AddUserSecrets<Program>()
            .Build();

        var connectionString = config["ServiceBusConnectionString"];

        var topicOption = new Option<string>("--topic", "The topic");
        var subscriptionOption = new Option<string>("--subscriber", "The subscriber");

        var rootCommand = new RootCommand("AzureServiceBusUtil");
        rootCommand.AddGlobalOption(topicOption);
        rootCommand.AddGlobalOption(subscriptionOption);

        rootCommand.SetHandler(_ => { Console.WriteLine("Let's do some things"); });
        AddCommand(rootCommand,"list-subscriptions", "Lists all topics and subscriptions", _ => ServiceBusProcessor.ListTopicsAndSubscriptionsAsync(connectionString));


        var purgeCommand = new Command("purge-dlq", "Remove all dead-lettered messages from the topic's DLQ");
        purgeCommand.SetHandler(async (topicOptionValue, subOptionValue) =>
        {
            await ServiceBusProcessor.PurgeDeadLetterQueueAsync(connectionString, topicOptionValue, subOptionValue);
        },topicOption, subscriptionOption);
        rootCommand.Add(purgeCommand);


        var purgeOldCommand = new Command("purge-dlq-old", "Remove all dead-lettered messages older than 1 month from the topic's DLQ");
        purgeOldCommand.SetHandler(async (topicOptionValue, subOptionValue) =>
        {
            await ServiceBusProcessor.PurgeDeadLetterQueueOlderThanOneMonth(connectionString, topicOptionValue, subOptionValue);
        }, topicOption, subscriptionOption);
        rootCommand.Add(purgeOldCommand);


        var purgeQueueCommand = new Command("purge-queue", "Remove all unprocessed records from queue");
        purgeQueueCommand.SetHandler(async (topicOptionValue) =>
        {
            await ServiceBusProcessor.PurgeQueueAsync(connectionString, topicOptionValue);
        }, topicOption);
        rootCommand.Add(purgeQueueCommand);


        var publishBatch = new Command("make-it-rain", "Publish batch of 1000 test messages");
        publishBatch.SetHandler(async (topicOptionValue) =>
        {
            await PublishTestData(connectionString, topicOptionValue);
        }, topicOption);
        rootCommand.Add(publishBatch);


        await rootCommand.InvokeAsync(args);
    }

    private static void AddCommand(Command root, string commandName, string commandDescription, Func<InvocationContext, Task> handler)
    {
        var listCommand = new Command(commandName, commandDescription);
        listCommand.SetHandler(handler);
        root.Add(listCommand);
    }

    private static async Task PublishTestData(string connectionString, string topic)
    {
        var messages = new List<TestEvent>();
        for (var i = 0; i < 1000; i++)
        {
            messages.Add(new TestEvent { Id = i, Name = $"Test message {i}" });
        }
        await ServiceBusProcessor.SendBatchAsync(messages, connectionString,topic);
    }
}
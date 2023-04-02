using Azure.Messaging.ServiceBus;

// The fields we need to write a message.

const string connectionString = "YOUR CONNECTION STRING";
const string duplicateQueueName = "queue-contains-duplicates";
const string dedupedQueueName = "queue-is-deduped";
const int duplicateCount = 100;

string messageText = Guid.NewGuid().ToString();
string messageId = messageText;

// The message ID is just the text of the message. In our use case, this would be
// the incident ID.

Console.WriteLine("Writing to the non-deduped queue....");
await SendDuplicateMessages(
    connectionString, duplicateQueueName, messageText, messageId, duplicateCount);

Console.WriteLine("Writing to the deuped queue....");
await SendDuplicateMessages(
    connectionString, dedupedQueueName, messageText, messageText, duplicateCount);

Console.WriteLine("Send complete. Press any key to exit.");
Console.ReadKey();

async Task SendDuplicateMessages(
    string connectionString,
    string queueName,
    string messageText,
    string messageId,
    int duplicateCount)
{
    var clientOptions = new ServiceBusClientOptions
    {
        TransportType = ServiceBusTransportType.AmqpWebSockets
    };

    // Create the client and sender.

    ServiceBusClient client = new ServiceBusClient(connectionString, clientOptions);
    ServiceBusSender sender = client.CreateSender(queueName);

    // Create a batch and add messages to it

    using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

    for (int i = 1; i <= duplicateCount; i++)
    {
        ServiceBusMessage serviceBusMessage = new ServiceBusMessage(messageText);
        serviceBusMessage.MessageId = messageId;

        if (!messageBatch.TryAddMessage(serviceBusMessage))
        {
            throw new Exception($"The message {i} is too large to fit in the batch.");
        }
    }

    try
    {
        // Send the batch.

        await sender.SendMessagesAsync(messageBatch);

    }
    finally
    {
        // Calling DisposeAsync on client types is required to ensure that network
        // resources and other unmanaged objects are properly cleaned up.
        await sender.DisposeAsync();
        await client.DisposeAsync();
    }
}
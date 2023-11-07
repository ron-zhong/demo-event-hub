using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using System.Text;

Console.WriteLine("event-receiver: wait for events.....");

// Create a blob container client that the event processor will use
// TODO: Replace <STORAGE_ACCOUNT_NAME> and <BLOB_CONTATINAER_NAME> with actual names
BlobContainerClient storageClient = new BlobContainerClient(
    new Uri("https://poceventstorage.blob.core.windows.net/poc-event-storage"),
    new DefaultAzureCredential());

// Create an event processor client to process events in the event hub
// TODO: Replace the <EVENT_HUBS_NAMESPACE> and <HUB_NAME> placeholder values
var processor = new EventProcessorClient(
    storageClient,
    EventHubConsumerClient.DefaultConsumerGroupName,
    "poc-event-namespace.servicebus.windows.net",
    "poc-event-hub",
    new DefaultAzureCredential());

// Register handlers for processing events and handling errors
processor.ProcessEventAsync += ProcessEventHandler;
processor.ProcessErrorAsync += ProcessErrorHandler;

while (true)
{
    // Start the processing
    await processor.StartProcessingAsync();

    // Wait for 30 seconds for the events to be processed
    await Task.Delay(TimeSpan.FromSeconds(3));

    // Stop the processing
    await processor.StopProcessingAsync();
}

Task ProcessEventHandler(ProcessEventArgs eventArgs)
{
    // Write the body of the event to the console window
    Console.WriteLine("\tReceived event: {0} at {1}",
        Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()),
        DateTime.Now.ToLongTimeString());
    //Console.ReadLine();
    return Task.CompletedTask;
}

Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
{
    // Write details about the error to the console window
    Console.WriteLine($"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
    Console.WriteLine(eventArgs.Exception.Message);
    //Console.ReadLine();
    return Task.CompletedTask;
}
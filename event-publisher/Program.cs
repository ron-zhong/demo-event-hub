using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System.Text;

Console.WriteLine("event-publisher: start sending events.....");

// number of events to be sent to the event hub
int numOfEvents = 100;

// The Event Hubs client types are safe to cache and use as a singleton for the lifetime
// of the application, which is best practice when events are being published or read regularly.
// TODO: Replace the <EVENT_HUB_NAMESPACE> and <HUB_NAME> placeholder values
EventHubProducerClient producerClient = new EventHubProducerClient(
    "poc-event-namespace.servicebus.windows.net",
    "poc-event-hub",
    new DefaultAzureCredential());

int i = 1;

while (i <= numOfEvents)
{
    // Create a batch of events 
    using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

    while( i % 10 != 0)
    {
        Console.WriteLine($"New event {i} at {DateTime.Now.ToLongTimeString()}");

        if (!eventBatch.TryAdd(
            new EventData(
                Encoding.UTF8.GetBytes($"Event {i} at {DateTime.Now.ToLongTimeString()}")
                )
            ))
        {
            // if it is too large for the batch
            throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
        }

        ++i;
    }

    try
    {
        // Use the producer client to send the batch of events to the event hub
        await producerClient.SendAsync(eventBatch);
        Console.WriteLine($"A batch of {i} events has been published.");
    }
    finally
    {
        //await producerClient.DisposeAsync();
    }

    Console.WriteLine("Press enter to restart");
    Console.ReadLine();
}

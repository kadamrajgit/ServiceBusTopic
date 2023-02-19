using System;
using Azure.Messaging.ServiceBus;
using Azure.Identity;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus.Administration;
using System.Text;
using Newtonsoft.Json;

namespace ServiceBusTopic
{
    internal class Program
    {
        static ServiceBusAdministrationClient adminClient;
        // the client that owns the connection and can be used to create senders and receivers
        static ServiceBusClient client;

        // the sender used to publish messages to the topic
        static ServiceBusSender sender;

        // the receiver used to receive messages from the subscription 
        static ServiceBusReceiver receiver;
        // connection string to the Service Bus namespace
        // static readonly string connectionString = "<YOUR SERVICE BUS NAMESPACE - CONNECTION STRING>";
        static string connectionString = "Endpoint=sb://aravkadam.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=GhIeYOGZjWyiVLsC4FeRQeesqu6BQ3zqv+ASbDxbxbE=";

        // name of the Service Bus topic
        static readonly string topicName = "RajeshKadamTopic";

        // names of subscriptions to the topic
        static readonly string subscriptionAllOrders = "AllOrders";
        static readonly string subscriptionColorBlueSize10Orders = "ColorBlueSize10Orders";
        static readonly string subscriptionColorRed = "ColorRed";
        static readonly string subscriptionHighPriorityRedOrders = "HighPriorityRedOrders";

        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            Console.WriteLine("Creating the Service Bus Administration Client object");
            adminClient = new ServiceBusAdministrationClient(connectionString);

            Console.WriteLine($"Creating the topic {topicName}");
            await adminClient.CreateTopicAsync(topicName);

            Console.WriteLine($"Creating the subscription {subscriptionAllOrders} for the topic with a True filter ");
            // Create a True Rule filter with an expression that always evaluates to true
            // It's equivalent to using SQL rule filter with 1=1 as the expression
            await adminClient.CreateSubscriptionAsync(
                    new CreateSubscriptionOptions(topicName, subscriptionAllOrders),
                    new CreateRuleOptions("AllOrders", new TrueRuleFilter()));


            Console.WriteLine($"Creating the subscription {subscriptionColorBlueSize10Orders} with a SQL filter");
            // Create a SQL filter with color set to blue and quantity to 10
            await adminClient.CreateSubscriptionAsync(
                    new CreateSubscriptionOptions(topicName, subscriptionColorBlueSize10Orders),
                    new CreateRuleOptions("BlueSize10Orders", new SqlRuleFilter("color='blue' AND quantity=10")));


            Console.WriteLine($"Creating the subscription {subscriptionColorRed} with a SQL filter");
            // Create a SQL filter with color equals to red and a SQL action with a set of statements
            await adminClient.CreateSubscriptionAsync(topicName, subscriptionColorRed);
            // remove the $Default rule
            await adminClient.DeleteRuleAsync(topicName, subscriptionColorRed, "$Default");
            // now create the new rule. notice that user. prefix is used for the user/application property
            await adminClient.CreateRuleAsync(topicName, subscriptionColorRed, new CreateRuleOptions
            {
                Name = "RedOrdersWithAction",
                Filter = new SqlRuleFilter("user.color='red'"),
                Action = new SqlRuleAction("SET quantity = quantity / 2; REMOVE priority;SET sys.CorrelationId = 'low';")

            }
            );

            Console.WriteLine($"Creating the subscription {subscriptionHighPriorityRedOrders} with a correlation filter");
            // Create a correlation filter with color set to Red and priority set to High
            await adminClient.CreateSubscriptionAsync(
                    new CreateSubscriptionOptions(topicName, subscriptionHighPriorityRedOrders),
                    new CreateRuleOptions("HighPriorityRedOrders", new CorrelationRuleFilter() { Subject = "red", CorrelationId = "high" }));

           
           // await adminClient.DeleteTopicAsync(topicName);

            Program app = new Program();
            await app.SendAndReceiveTestsAsync(connectionString);

        }












        public async Task SendAndReceiveTestsAsync(string connectionString)
        {
            // This sample demonstrates how to use advanced filters with ServiceBus topics and subscriptions.
            // The sample creates a topic and 3 subscriptions with different filter definitions.
            // Each receiver will receive matching messages depending on the filter associated with a subscription.

            // Send sample messages.
            await this.SendMessagesToTopicAsync(connectionString);

            // Receive messages from subscriptions.
            await this.ReceiveAllMessageFromSubscription(connectionString, subscriptionAllOrders);
            await this.ReceiveAllMessageFromSubscription(connectionString, subscriptionColorBlueSize10Orders);
            await this.ReceiveAllMessageFromSubscription(connectionString, subscriptionColorRed);
            await this.ReceiveAllMessageFromSubscription(connectionString, subscriptionHighPriorityRedOrders);
        }
        async Task SendMessagesToTopicAsync(string connectionString)
        {
            // Create the clients that we'll use for sending and processing messages.
            client = new ServiceBusClient(connectionString);
            sender = client.CreateSender(topicName);

            Console.WriteLine("\nSending orders to topic.");

            // Now we can start sending orders.
            await Task.WhenAll(
                SendOrder(sender, new Order()),
                SendOrder(sender, new Order { Color = "blue", Quantity = 5, Priority = "low" }),
                SendOrder(sender, new Order { Color = "red", Quantity = 10, Priority = "high" }),
                SendOrder(sender, new Order { Color = "yellow", Quantity = 5, Priority = "low" }),
                SendOrder(sender, new Order { Color = "blue", Quantity = 10, Priority = "low" }),
                SendOrder(sender, new Order { Color = "blue", Quantity = 5, Priority = "high" }),
                SendOrder(sender, new Order { Color = "blue", Quantity = 10, Priority = "low" }),
                SendOrder(sender, new Order { Color = "red", Quantity = 5, Priority = "low" }),
                SendOrder(sender, new Order { Color = "red", Quantity = 10, Priority = "low" }),
                SendOrder(sender, new Order { Color = "red", Quantity = 5, Priority = "low" }),
                SendOrder(sender, new Order { Color = "yellow", Quantity = 10, Priority = "high" }),
                SendOrder(sender, new Order { Color = "yellow", Quantity = 5, Priority = "low" }),
                SendOrder(sender, new Order { Color = "yellow", Quantity = 10, Priority = "low" })
                );

            Console.WriteLine("All messages sent.");
        }


        async Task SendOrder(ServiceBusSender sender, Order order)
        {
            var message = new ServiceBusMessage(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(order)))
            {
                CorrelationId = order.Priority,
                Subject = order.Color,
                ApplicationProperties =
                {
                    { "color", order.Color },
                    { "quantity", order.Quantity },
                    { "priority", order.Priority }
                }
            };
            await sender.SendMessageAsync(message);

            Console.WriteLine("Sent order with Color={0}, Quantity={1}, Priority={2}", order.Color, order.Quantity, order.Priority);
        }

        async Task ReceiveAllMessageFromSubscription(string connectionString, string subsName)
        {
            var receivedMessages = 0;

            receiver = client.CreateReceiver(topicName, subsName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });

            // Create a receiver from the subscription client and receive all messages.
            Console.WriteLine("\nReceiving messages from subscription {0}.", subsName);

            while (true)
            {
                var receivedMessage = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
                if (receivedMessage != null)
                {
                    foreach (var prop in receivedMessage.ApplicationProperties)
                    {
                        Console.Write("{0}={1},", prop.Key, prop.Value);
                    }
                    Console.WriteLine("CorrelationId={0}", receivedMessage.CorrelationId);
                    receivedMessages++;
                }
                else
                {
                    // No more messages to receive.
                    break;
                }
            }
            Console.WriteLine("Received {0} messages from subscription {1}.", receivedMessages, subsName);
        }


    }

    class Order
    {
        public string Color
        {
            get;
            set;
        }

        public int Quantity
        {
            get;
            set;
        }

        public string Priority
        {
            get;
            set;
        }
    }





}


﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs.Host.TestCommon;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;

namespace Microsoft.Azure.WebJobs.Host.EndToEndTests
{
    public class ServiceBusEndToEndTests : IDisposable
    {
        private const string SecondaryConnectionStringKey = "ServiceBusSecondary";

        private const string Prefix = "core-test-";
        private const string FirstQueueName = Prefix + "queue1";
        private const string SecondQueueName = Prefix + "queue2";
        private const string BinderQueueName = Prefix + "queue3";

        private const string TopicName = Prefix + "topic1";
        private const string TopicSubscriptionName1 = "sub1";
        private const string TopicSubscriptionName2 = "sub2";

        private const string TriggerDetailsMessageStart = "Trigger Details:";
        private const string DrainingQueueMessageBody = "queue-message-draining-no-sessions-1";
        private const string DrainingTopicMessageBody = "topic-message-draining-no-sessions-1";

        private const int SBTimeout = 120 * 1000;
        private const int DrainSleepTime = 60 * 1000;
        private const int MaxAutoRenewDurationMin = 5;
        internal static TimeSpan HostShutdownTimeout = TimeSpan.FromSeconds(120);

        private static EventWaitHandle _topicSubscriptionCalled1;
        private static EventWaitHandle _topicSubscriptionCalled2;
        private static EventWaitHandle _eventWait;
        private static EventWaitHandle _drainValidationPreDelay;
        private static EventWaitHandle _drainValidationPostDelay;

        // These two variables will be checked at the end of the test
        private static string _resultMessage1;
        private static string _resultMessage2;

        private readonly RandomNameResolver _nameResolver;
        private readonly string _primaryConnectionString;
        private readonly string _secondaryConnectionString;

        public ServiceBusEndToEndTests()
        {
            var config = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .AddTestSettings()
                .Build();

            _eventWait = new ManualResetEvent(initialState: false);

            _primaryConnectionString = config.GetConnectionStringOrSetting(ServiceBus.Constants.DefaultConnectionStringName);
            _secondaryConnectionString = config.GetConnectionStringOrSetting(SecondaryConnectionStringKey);

            _nameResolver = new RandomNameResolver();

            Cleanup().GetAwaiter().GetResult();
        }

        [Fact]
        public async Task ServiceBusEndToEnd()
        {
            await ServiceBusEndToEndInternal<ServiceBusTestJobs>();
        }

        [Fact]
        public async Task ServiceBusBinderTest()
        {
            var hostType = typeof(ServiceBusTestJobs);
            var host = CreateHost<ServiceBusTestJobs>();
            var method = typeof(ServiceBusTestJobs).GetMethod("ServiceBusBinderTest");

            int numMessages = 10;
            var args = new { message = "Test Message", numMessages = numMessages };
            var jobHost = host.GetJobHost<ServiceBusTestJobs>();
            await jobHost.CallAsync(method, args);
            await jobHost.CallAsync(method, args);
            await jobHost.CallAsync(method, args);

            var count = await CleanUpEntity(BinderQueueName);

            Assert.Equal(numMessages * 3, count);
        }

        [Fact]
        public async Task CustomMessageProcessorTest()
        {
            IHost host = new HostBuilder()
                .ConfigureDefaultTestHost<ServiceBusTestJobs>(b =>
                {
                    b.AddServiceBus();
                })
                .ConfigureServices(services =>
                {
                    services.AddSingleton<MessagingProvider, CustomMessagingProvider>();
                })
                .ConfigureServices(s =>
                {
                    s.Configure<HostOptions>(opts => opts.ShutdownTimeout = HostShutdownTimeout);
                })
                .Build();

            var loggerProvider = host.GetTestLoggerProvider();

            await ServiceBusEndToEndInternal<ServiceBusTestJobs>(host: host);

            // in addition to verifying that our custom processor was called, we're also
            // verifying here that extensions can log
            IEnumerable<LogMessage> messages = loggerProvider.GetAllLogMessages().Where(m => m.Category == CustomMessagingProvider.CustomMessagingCategory);
            Assert.Equal(4, messages.Count(p => p.FormattedMessage.Contains("Custom processor Begin called!")));
            Assert.Equal(4, messages.Count(p => p.FormattedMessage.Contains("Custom processor End called!")));
        }

        [Fact]
        public async Task MultipleAccountTest()
        {
            IHost host = new HostBuilder()
               .ConfigureDefaultTestHost<ServiceBusTestJobs>(b =>
               {
                   b.AddServiceBus();
               }, nameResolver: _nameResolver)
               .ConfigureServices(services =>
               {
                   services.AddSingleton<MessagingProvider, CustomMessagingProvider>();
               })
               .ConfigureServices(s =>
               {
                   s.Configure<HostOptions>(opts => opts.ShutdownTimeout = HostShutdownTimeout);
               })
               .Build();

            await WriteQueueMessage(_secondaryConnectionString, FirstQueueName, "Test");

            _topicSubscriptionCalled1 = new ManualResetEvent(initialState: false);
            _topicSubscriptionCalled2 = new ManualResetEvent(initialState: false);

            await host.StartAsync();

            _topicSubscriptionCalled1.WaitOne(SBTimeout);
            _topicSubscriptionCalled2.WaitOne(SBTimeout);

            // ensure all logs have had a chance to flush
            await Task.Delay(3000);

            // Wait for the host to terminate
            await host.StopAsync();
            host.Dispose();

            Assert.Equal("Test-SBQueue2SBQueue-SBQueue2SBTopic-topic-1", _resultMessage1);
            Assert.Equal("Test-SBQueue2SBQueue-SBQueue2SBTopic-topic-2", _resultMessage2);
        }

        [Fact]
        public async Task TestBatch_String()
        {
            await TestMultiple<ServiceBusMultipleMessagesTestJob_BindToStringArray>();
        }

        [Fact]
        public async Task TestBatch_Messages()
        {
            await TestMultiple<ServiceBusMultipleMessagesTestJob_BindToMessageArray>();
        }

        [Fact]
        public async Task TestBatch_JsonPoco()
        {
            await TestMultiple<ServiceBusMultipleMessagesTestJob_BindToPocoArray>();
        }

        [Fact]
        public async Task TestBatch_DataContractPoco()
        {
            await TestMultiple<ServiceBusMultipleMessagesTestJob_BindToPocoArray>(true);
        }

        [Fact]
        public async Task BindToPoco()
        {
            var host = BuildTestHost<ServiceBusArgumentBindingJob>();

            await WriteQueueMessage(_primaryConnectionString, FirstQueueName, "{ Name: 'foo', Value: 'bar' }");

            await host.StartAsync();

            bool result = _eventWait.WaitOne(SBTimeout);
            Assert.True(result);

            var logs = host.GetTestLoggerProvider().GetAllLogMessages().Select(p => p.FormattedMessage);
            Assert.Contains("PocoValues(foo,bar)", logs);

            await host.StopAsync();
            host.Dispose();
        }

        [Fact]
        public async Task BindToString()
        {
            var host = BuildTestHost<ServiceBusArgumentBindingJob>();

            var method = typeof(ServiceBusArgumentBindingJob).GetMethod(nameof(ServiceBusArgumentBindingJob.BindToString), BindingFlags.Static | BindingFlags.Public);
            var jobHost = host.GetJobHost();
            await jobHost.CallAsync(method, new { input = "foobar" });

            bool result = _eventWait.WaitOne(SBTimeout);
            Assert.True(result);

            var logs = host.GetTestLoggerProvider().GetAllLogMessages().Select(p => p.FormattedMessage);
            Assert.Contains("Input(foobar)", logs);
        }

        [Fact]
        public async Task MessageDrainingQueue()
        {
            await TestSingleDrainMode<DrainModeValidationFunctions>(true);
        }

        [Fact]
        public async Task MessageDrainingTopic()
        {
            await TestSingleDrainMode<DrainModeValidationFunctions>(false);
        }

        [Fact]
        public async Task MessageDrainingQueueBatch()
        {
            await TestMultipleDrainMode<DrainModeValidationFunctions>(true);
        }

        [Fact]
        public async Task MessageDrainingTopicBatch()
        {
            await TestMultipleDrainMode<DrainModeValidationFunctions>(false);
        }

        /*
         * Helper functions
         */

        private async Task TestSingleDrainMode<T>(bool sendToQueue)
        {
            var host = BuildTestHost<DrainModeValidationFunctions>();
            await host.StartAsync();

            _drainValidationPreDelay = new ManualResetEvent(initialState: false);
            _drainValidationPostDelay = new ManualResetEvent(initialState: false);

            if (sendToQueue)
            {
                await WriteQueueMessage(_primaryConnectionString, FirstQueueName, DrainingQueueMessageBody);
            }
            else
            {
                await WriteTopicMessage(_primaryConnectionString, TopicName, DrainingTopicMessageBody);
            }

            // Wait to ensure function invocatoin has started before draining messages
            Assert.True(_drainValidationPreDelay.WaitOne(SBTimeout));

            // Start draining in-flight messages
            var drainModeManager = host.Services.GetService<IDrainModeManager>();
            await drainModeManager.EnableDrainModeAsync(CancellationToken.None);

            // Validate that function execution was allowed to complete
            Assert.True(_drainValidationPostDelay.WaitOne(DrainSleepTime + SBTimeout));

            await host.StopAsync();
            host.Dispose();
        }

        private async Task TestMultiple<T>(bool isXml = false)
        {
            IHost host = BuildTestHost<T>();

            if (isXml)
            {
                await WriteQueueMessage(_primaryConnectionString, FirstQueueName, new TestPoco() { Name = "Test1", Value = "Value" });
                await WriteQueueMessage(_primaryConnectionString, FirstQueueName, new TestPoco() { Name = "Test2", Value = "Value" });
            }
            else
            {
                await WriteQueueMessage(_primaryConnectionString, FirstQueueName, "{'Name': 'Test1', 'Value': 'Value'}");
                await WriteQueueMessage(_primaryConnectionString, FirstQueueName, "{'Name': 'Test2', 'Value': 'Value'}");
            }

            _topicSubscriptionCalled1 = new ManualResetEvent(initialState: false);

            await host.StartAsync();

            bool result = _topicSubscriptionCalled1.WaitOne(SBTimeout);
            Assert.True(result);

            // ensure message are completed
            await Task.Delay(2000);

            // Wait for the host to terminate
            await host.StopAsync();
            host.Dispose();
        }

        private async Task TestMultipleDrainMode<T>(bool sendToQueue)
        {
            IHost host = new HostBuilder()
               .ConfigureDefaultTestHost<T>(b =>
               {
                   b.AddServiceBus();
               }, nameResolver: _nameResolver)
               .ConfigureServices(s =>
               {
                   s.Configure<HostOptions>(opts => opts.ShutdownTimeout = HostShutdownTimeout);
               })
               .Build();

            await host.StartAsync();

            _drainValidationPreDelay = new ManualResetEvent(initialState: false);
            _drainValidationPostDelay = new ManualResetEvent(initialState: false);

            if (sendToQueue)
            {
                await ServiceBusEndToEndTests.WriteQueueMessage(_primaryConnectionString, FirstQueueName, "{'Name': 'Test1', 'Value': 'Value'}");
                await ServiceBusEndToEndTests.WriteQueueMessage(_primaryConnectionString, FirstQueueName, "{'Name': 'Test2', 'Value': 'Value'}");
            }
            else
            {
                await ServiceBusEndToEndTests.WriteTopicMessage(_primaryConnectionString, TopicName, "{'Name': 'Test1', 'Value': 'Value'}");
                await ServiceBusEndToEndTests.WriteTopicMessage(_primaryConnectionString, TopicName, "{'Name': 'Test2', 'Value': 'Value'}");
            }

            // Wait to ensure function invocatoin has started before draining messages
            Assert.True(_drainValidationPreDelay.WaitOne(SBTimeout));

            // Start draining in-flight messages
            var drainModeManager = host.Services.GetService<IDrainModeManager>();
            await drainModeManager.EnableDrainModeAsync(CancellationToken.None);

            // Validate that function execution was allowed to complete
            Assert.True(_drainValidationPostDelay.WaitOne(DrainSleepTime + SBTimeout));

            // Wait for the host to terminate
            await host.StopAsync();
            host.Dispose();
        }

        private async Task<int> CleanUpEntity(string queueName, string connectionString = null)
        {
            var messageReceiver = new MessageReceiver(!string.IsNullOrEmpty(connectionString) ? connectionString : _primaryConnectionString, queueName, ReceiveMode.ReceiveAndDelete);
            Message message;
            int count = 0;

            do
            {
                message = await messageReceiver.ReceiveAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                if (message != null)
                {
                    count++;
                }
                else
                {
                    break;
                }
            } while (true);

            await messageReceiver.CloseAsync();

            return count;
        }

        private async Task Cleanup()
        {
            var tasks = new List<Task>
            {
                CleanUpEntity(FirstQueueName),
                CleanUpEntity(SecondQueueName),
                CleanUpEntity(BinderQueueName),
                CleanUpEntity(FirstQueueName, _secondaryConnectionString),
                CleanUpEntity(EntityNameHelper.FormatSubscriptionPath(TopicName, TopicSubscriptionName1)),
                CleanUpEntity(EntityNameHelper.FormatSubscriptionPath(TopicName, TopicSubscriptionName2))
            };

            await Task.WhenAll(tasks);
        }

        private IHost CreateHost<T>()
        {
            return new HostBuilder()
                .ConfigureDefaultTestHost<T>(b =>
                {
                    b.AddServiceBus();
                })
                .ConfigureServices(services =>
                {
                    services.AddSingleton<INameResolver>(_nameResolver);
                })
                .ConfigureServices(s =>
                {
                    s.Configure<HostOptions>(opts => opts.ShutdownTimeout = HostShutdownTimeout);
                })
                .Build();
        }

        private async Task ServiceBusEndToEndInternal<T>(IHost host = null)
        {
            if (host == null)
            {
                host = CreateHost<T>();
            }

            var jobContainerType = typeof(T);

            await WriteQueueMessage(_primaryConnectionString, FirstQueueName, "E2E");

            _topicSubscriptionCalled1 = new ManualResetEvent(initialState: false);
            _topicSubscriptionCalled2 = new ManualResetEvent(initialState: false);

            using (host)
            {
                await host.StartAsync();

                _topicSubscriptionCalled1.WaitOne(SBTimeout);
                _topicSubscriptionCalled2.WaitOne(SBTimeout);

                // ensure all logs have had a chance to flush
                await Task.Delay(4000);

                // Wait for the host to terminate
                await host.StopAsync();

                Assert.Equal("E2E-SBQueue2SBQueue-SBQueue2SBTopic-topic-1", _resultMessage1);
                Assert.Equal("E2E-SBQueue2SBQueue-SBQueue2SBTopic-topic-2", _resultMessage2);

                IEnumerable<LogMessage> logMessages = host.GetTestLoggerProvider()
                    .GetAllLogMessages();

                // filter out anything from the custom processor for easier validation.
                IEnumerable<LogMessage> consoleOutput = logMessages
                    .Where(m => m.Category != CustomMessagingProvider.CustomMessagingCategory);

                Assert.DoesNotContain(consoleOutput, p => p.Level == LogLevel.Error);

                string[] consoleOutputLines = consoleOutput
                    .Where(p => p.FormattedMessage != null)
                    .SelectMany(p => p.FormattedMessage.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries))
                    .OrderBy(p => p)
                    .ToArray();

                string[] expectedOutputLines = new string[]
                {
                   "Found the following functions:",
                    $"{jobContainerType.FullName}.SBQueue2SBQueue",
                    $"{jobContainerType.FullName}.MultipleAccounts",
                    $"{jobContainerType.FullName}.SBQueue2SBTopic",
                    $"{jobContainerType.FullName}.SBTopicListener1",
                    $"{jobContainerType.FullName}.SBTopicListener2",
                    $"{jobContainerType.FullName}.ServiceBusBinderTest",
                    "Job host started",
                    $"Executing '{jobContainerType.Name}.SBQueue2SBQueue' (Reason='', Id=",
                    $"Executed '{jobContainerType.Name}.SBQueue2SBQueue' (Succeeded, Id=",
                    $"Trigger Details:",
                    $"Executing '{jobContainerType.Name}.SBQueue2SBTopic' (Reason='', Id=",
                    $"Executed '{jobContainerType.Name}.SBQueue2SBTopic' (Succeeded, Id=",
                    $"Trigger Details:",
                    $"Executing '{jobContainerType.Name}.SBTopicListener1' (Reason='', Id=",
                    $"Executed '{jobContainerType.Name}.SBTopicListener1' (Succeeded, Id=",
                    $"Trigger Details:",
                    $"Executing '{jobContainerType.Name}.SBTopicListener2' (Reason='', Id=",
                    $"Executed '{jobContainerType.Name}.SBTopicListener2' (Succeeded, Id=",
                    $"Trigger Details:",
                    "Job host stopped",
                    "Starting JobHost",
                    "Stopping JobHost",
                    "FunctionResultAggregatorOptions",
                    "{",
                    "  \"BatchSize\": 1000",
                    "  \"FlushTimeout\": \"00:00:30\",",
                    "  \"IsEnabled\": true",
                    "}",
                    "LoggerFilterOptions",
                    "{",
                    "  \"MinLevel\": \"Information\"",
                    "  \"Rules\": []",
                    "}",
                    "ServiceBusOptions",
                    "{",
                    "  \"PrefetchCount\": 0,",
                    "  \"MessageHandlerOptions\": {",
                    "      \"AutoComplete\": true,",
                    "      \"MaxAutoRenewDuration\": \"00:05:00\",",
                    $"      \"MaxConcurrentCalls\": {16 * Utility.GetProcessorCount()}",
                    "  }",
                    "  \"SessionHandlerOptions\": {",
                    "      \"MaxAutoRenewDuration\": \"00:05:00\",",
                    "      \"MessageWaitTimeout\": \"00:01:00\",",
                    "      \"MaxConcurrentSessions\": 2000",
                    "      \"AutoComplete\": true",
                    "  }",
                    "}",
                    "  \"BatchOptions\": {",
                    "      \"MaxMessageCount\": 1000,",
                    "      \"OperationTimeout\": \"00:01:00\",",
                    "      \"AutoComplete\": true",
                    "  }",
                    "SingletonOptions",
                    "{",
                    "  \"ListenerLockPeriod\": \"00:01:00\"",
                    "  \"ListenerLockRecoveryPollingInterval\": \"00:01:00\"",
                    "  \"LockAcquisitionPollingInterval\": \"00:00:05\"",
                    "  \"LockAcquisitionTimeout\": \"",
                    "  \"LockPeriod\": \"00:00:15\"",
                    "}",
                }.OrderBy(p => p).ToArray();

                expectedOutputLines = expectedOutputLines.Select(x => x.Replace(" ", string.Empty)).ToArray();
                consoleOutputLines = consoleOutputLines.Select(x => x.Replace(" ", string.Empty)).ToArray();

                Action<string>[] inspectors = expectedOutputLines.Select<string, Action<string>>(p => (string m) =>
                {
                    Assert.True(p.StartsWith(m) || m.StartsWith(p));
                }).ToArray();
                Assert.Collection(consoleOutputLines, inspectors);

                // Verify that trigger details are properly formatted
                string[] triggerDetailsConsoleOutput = consoleOutputLines
                    .Where(m => m.StartsWith(TriggerDetailsMessageStart)).ToArray();

                string expectedPattern = "Trigger Details: MessageId: (.*), DeliveryCount: [0-9]+, EnqueuedTime: (.*), LockedUntil: (.*)";

                foreach (string msg in triggerDetailsConsoleOutput)
                {
                    Assert.True(Regex.IsMatch(msg, expectedPattern), $"Expected trace event {expectedPattern} not found.");
                }
            }
        }

        public static async Task WriteQueueMessage(string connectionString, string queueName, string message, string sessionId = null)
        {
            QueueClient queueClient = new QueueClient(connectionString, queueName);
            Message messageObj = new Message(Encoding.UTF8.GetBytes(message));
            if (!string.IsNullOrEmpty(sessionId))
            {
                messageObj.SessionId = sessionId;
            }
            await queueClient.SendAsync(messageObj);
            await queueClient.CloseAsync();
        }

        public static async Task WriteQueueMessage(string connectionString, string queueName, TestPoco obj, string sessionId = null)
        {
            var serializer = new DataContractSerializer(typeof(TestPoco));
            byte[] payload = null;
            using (var memoryStream = new MemoryStream(10))
            {
                var xmlDictionaryWriter = XmlDictionaryWriter.CreateBinaryWriter(memoryStream, null, null, false);
                serializer.WriteObject(xmlDictionaryWriter, obj);
                xmlDictionaryWriter.Flush();
                memoryStream.Flush();
                memoryStream.Position = 0;
                payload = memoryStream.ToArray();
            }

            QueueClient queueClient = new QueueClient(connectionString, queueName);
            Message messageObj = new Message(payload);
            if (!string.IsNullOrEmpty(sessionId))
            {
                messageObj.SessionId = sessionId;
            }
            await queueClient.SendAsync(messageObj);
            await queueClient.CloseAsync();
        }

        public static async Task WriteTopicMessage(string connectionString, string topicName, string message, string sessionId = null)
        {
            TopicClient client = new TopicClient(connectionString, topicName);
            Message messageObj = new Message(Encoding.UTF8.GetBytes(message));
            if (!string.IsNullOrEmpty(sessionId))
            {
                messageObj.SessionId = sessionId;
            }
            await client.SendAsync(messageObj);
            await client.CloseAsync();
        }

        public abstract class ServiceBusTestJobsBase
        {
            protected static Message SBQueue2SBQueue_GetOutputMessage(string input)
            {
                input = input + "-SBQueue2SBQueue";
                return new Message
                {
                    ContentType = "text/plain",
                    Body = Encoding.UTF8.GetBytes(input)
                };
            }

            protected static Message SBQueue2SBTopic_GetOutputMessage(string input)
            {
                input = input + "-SBQueue2SBTopic";

                return new Message(Encoding.UTF8.GetBytes(input))
                {
                    ContentType = "text/plain"
                };
            }

            protected static void SBTopicListener1Impl(string input)
            {
                _resultMessage1 = input + "-topic-1";
                _topicSubscriptionCalled1.Set();
            }

            protected static void SBTopicListener2Impl(Message message)
            {
                using (Stream stream = new MemoryStream(message.Body))
                using (TextReader reader = new StreamReader(stream))
                {
                    _resultMessage2 = reader.ReadToEnd() + "-topic-2";
                }

                _topicSubscriptionCalled2.Set();
            }
        }

        public class ServiceBusTestJobs : ServiceBusTestJobsBase
        {
            // Passes service bus message from a queue to another queue
            public static async Task SBQueue2SBQueue(
                [ServiceBusTrigger(FirstQueueName)] string start, int deliveryCount,
                MessageReceiver messageReceiver,
                string lockToken,
                [ServiceBus(SecondQueueName)] MessageSender messageSender)
            {
                Assert.Equal(FirstQueueName, messageReceiver.Path);
                Assert.Equal(1, deliveryCount);

                // verify the message receiver and token are valid
                await messageReceiver.RenewLockAsync(lockToken);

                var message = SBQueue2SBQueue_GetOutputMessage(start);
                await messageSender.SendAsync(message);
            }

            // Passes a service bus message from a queue to topic using a brokered message 
            public static void SBQueue2SBTopic(
                [ServiceBusTrigger(SecondQueueName)] string message,
                [ServiceBus(TopicName)] out Message output)
            {
                output = SBQueue2SBTopic_GetOutputMessage(message);
            }

            // First listener for the topic
            public static void SBTopicListener1(
                [ServiceBusTrigger(TopicName, TopicSubscriptionName1)] string message,
                MessageReceiver messageReceiver,
                string lockToken)
            {
                SBTopicListener1Impl(message);
            }

            // Second listener for the topic
            // Just sprinkling Singleton here because previously we had a bug where this didn't work
            // for ServiceBus.
            [Singleton]
            public static void SBTopicListener2(
                [ServiceBusTrigger(TopicName, TopicSubscriptionName2)] Message message)
            {
                SBTopicListener2Impl(message);
            }

            // Demonstrate triggering on a queue in one account, and writing to a topic
            // in the primary subscription
            public static void MultipleAccounts(
                [ServiceBusTrigger(FirstQueueName, Connection = SecondaryConnectionStringKey)] string input,
                [ServiceBus(TopicName)] out string output)
            {
                output = input;
            }

            [NoAutomaticTrigger]
            public static async Task ServiceBusBinderTest(
                string message,
                int numMessages,
                Binder binder)
            {
                var attribute = new ServiceBusAttribute(BinderQueueName)
                {
                    EntityType = EntityType.Queue
                };

                var collector = await binder.BindAsync<IAsyncCollector<string>>(attribute);

                for (int i = 0; i < numMessages; i++)
                {
                    await collector.AddAsync(message + i);
                }

                await collector.FlushAsync();
            }
        }

        public class ServiceBusMultipleTestJobsBase
        {
            protected static bool firstReceived = false;
            protected static bool secondReceived = false;

            public static void ProcessMessages(string[] messages)
            {
                if (messages.Contains("{'Name': 'Test1', 'Value': 'Value'}"))
                {
                    firstReceived = true;
                }
                if (messages.Contains("{'Name': 'Test2', 'Value': 'Value'}"))
                {
                    secondReceived = true;
                }

                if (firstReceived && secondReceived)
                {
                    _topicSubscriptionCalled1.Set();
                }
            }
        }

        public class ServiceBusMultipleMessagesTestJob_BindToStringArray 
        {

            public static async Task SBQueue2SBQueue(
                [ServiceBusTrigger(FirstQueueName)] string[] messages,
                MessageReceiver messageReceiver, CancellationToken cancellationToken)
            {
                try
                {
                    Assert.Equal(FirstQueueName, messageReceiver.Path);
                    ServiceBusMultipleTestJobsBase.ProcessMessages(messages);
                    await Task.Delay(0, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }

        public class ServiceBusMultipleMessagesTestJob_BindToMessageArray 
        {

            public static void SBQueue2SBQueue(
                [ServiceBusTrigger(FirstQueueName)] Message[] array,
                MessageReceiver messageReceiver)
            {
                Assert.Equal(FirstQueueName, messageReceiver.Path);
                string[] messages = array.Select(x =>
                {
                    using (Stream stream = new MemoryStream(x.Body))
                    using (TextReader reader = new StreamReader(stream))
                    {
                        return reader.ReadToEnd();
                    }
                }).ToArray();
                ServiceBusMultipleTestJobsBase.ProcessMessages(messages);
            }
        }

        public class ServiceBusMultipleMessagesTestJob_BindToPocoArray
        {
            public static void SBQueue2SBQueue(
                [ServiceBusTrigger(FirstQueueName)] TestPoco[] array,
                MessageReceiver messageReceiver)
            {
                Assert.Equal(FirstQueueName, messageReceiver.Path);
                string[] messages = array.Select(x => "{'Name': '" + x.Name + "', 'Value': 'Value'}").ToArray();
                ServiceBusMultipleTestJobsBase.ProcessMessages(messages);
            }
        }

        public class ServiceBusArgumentBindingJob
        {
            public static void BindToPoco(
                [ServiceBusTrigger(FirstQueueName)] TestPoco input,
                string name, string value, string messageId,
                ILogger logger)
            {
                Assert.Equal(input.Name, name);
                Assert.Equal(input.Value, value);
                logger.LogInformation($"PocoValues({name},{value})");
                _eventWait.Set();
            }

            [NoAutomaticTrigger]
            public static void BindToString(
                [ServiceBusTrigger(FirstQueueName)] string input,
                string messageId,
                ILogger logger)
            {
                logger.LogInformation($"Input({input})");
                _eventWait.Set();
            }
        }

        public class DrainModeValidationFunctions
        {
            public async static void QueueNoSessions(
                [ServiceBusTrigger(FirstQueueName)] Message msg,
                string messageId,
                ILogger logger)
            {
                logger.LogInformation($"DrainModeValidationFunctions.QueueNoSessions: message data {msg.Body}");
                _drainValidationPreDelay.Set();
                // Simulate a long running function execution to validate that drain invocation allows this to complete
                await Task.Delay(DrainSleepTime);
                _drainValidationPostDelay.Set();
            }

            public async static void TopicNoSessions(
                [ServiceBusTrigger(TopicName, TopicSubscriptionName1)] Message msg,
                string messageId,
                ILogger logger)
            {
                logger.LogInformation($"DrainModeValidationFunctions.NoSessions: message data {msg.Body}");
                _drainValidationPreDelay.Set();
                // Simulate a long running function execution to validate that drain invocation allows this to complete
                await Task.Delay(DrainSleepTime);
                _drainValidationPostDelay.Set();
            }
        }

        private class CustomMessagingProvider : MessagingProvider
        {
            public const string CustomMessagingCategory = "CustomMessagingProvider";
            private readonly ILogger _logger;
            private readonly ServiceBusOptions _options;

            public CustomMessagingProvider(IOptions<ServiceBusOptions> serviceBusOptions, ILoggerFactory loggerFactory)
                : base(serviceBusOptions)
            {
                _options = serviceBusOptions.Value;
                _logger = loggerFactory?.CreateLogger(CustomMessagingCategory);
            }

            public override MessageProcessor CreateMessageProcessor(string entityPath, string connectionName = null)
            {
                var options = new MessageHandlerOptions(ExceptionReceivedHandler)
                {
                    MaxConcurrentCalls = 3,
                    MaxAutoRenewDuration = TimeSpan.FromMinutes(MaxAutoRenewDurationMin)
                };

                var messageReceiver = new MessageReceiver(_options.ConnectionString, entityPath);

                return new CustomMessageProcessor(messageReceiver, options, _logger);
            }

            private class CustomMessageProcessor : MessageProcessor
            {
                private readonly ILogger _logger;

                public CustomMessageProcessor(MessageReceiver messageReceiver, MessageHandlerOptions messageOptions, ILogger logger)
                    : base(messageReceiver, messageOptions)
                {
                    _logger = logger;
                }

                public override async Task<bool> BeginProcessingMessageAsync(Message message, CancellationToken cancellationToken)
                {
                    _logger?.LogInformation("Custom processor Begin called!");
                    return await base.BeginProcessingMessageAsync(message, cancellationToken);
                }

                public override async Task CompleteProcessingMessageAsync(Message message, Executors.FunctionResult result, CancellationToken cancellationToken)
                {
                    _logger?.LogInformation("Custom processor End called!");
                    await base.CompleteProcessingMessageAsync(message, result, cancellationToken);
                }
            }

            private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs eventArgs)
            {
                return Task.CompletedTask;
            }
        }

        private IHost BuildTestHost<TJobClass>()
        {
            IHost host = new HostBuilder()
               .ConfigureDefaultTestHost<TJobClass>(b =>
               {
                   b.AddServiceBus();
               }, nameResolver: _nameResolver)
               .ConfigureServices(s =>
               {
                   s.Configure<HostOptions>(opts => opts.ShutdownTimeout = HostShutdownTimeout);
               })
               .Build();

            return host;
        }

        public void Dispose()
        {
            Cleanup().GetAwaiter().GetResult();
        }
    }

    public class TestPoco
    {
        public string Name { get; set; }
        public string Value { get; set; }
    }
}
// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Options;
using Xunit;

namespace Microsoft.Azure.WebJobs.ServiceBus.UnitTests
{
    public class MessagingProviderTests
    {
        private static string defaultConnection = "Endpoint=sb://default.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123=";
        private static string defaultEndpoint = "sb://default.servicebus.windows.net/";
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void CreateMessageReceiver_ReturnsExpectedReceiver(bool useManagedIdentity)
        {
            var connection = new ServiceBusConnection(useManagedIdentity ? defaultEndpoint : defaultConnection, useManagedIdentity);
            var config = new ServiceBusOptions
            {
                ConnectionString = defaultConnection
            };
            var provider = new MessagingProvider(new OptionsWrapper<ServiceBusOptions>(config));
            var receiver = provider.CreateMessageReceiver("entityPath", connection);
            Assert.Equal("entityPath", receiver.Path);

            var receiver2 = provider.CreateMessageReceiver("entityPath", connection);
            Assert.Same(receiver, receiver2);

            config.PrefetchCount = 100;
            receiver = provider.CreateMessageReceiver("entityPath1", connection);
            Assert.Equal(100, receiver.PrefetchCount);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void CreateClientEntity_ReturnsExpectedReceiver(bool useManagedIdentity)
        {
            var connection = new ServiceBusConnection(useManagedIdentity ? defaultEndpoint : defaultConnection, useManagedIdentity);
            var config = new ServiceBusOptions
            {
                ConnectionString = defaultConnection
            };
            var provider = new MessagingProvider(new OptionsWrapper<ServiceBusOptions>(config));
            var clientEntity = provider.CreateClientEntity("entityPath", connection);
            Assert.Equal("entityPath", clientEntity.Path);

            var receiver2 = provider.CreateClientEntity("entityPath", connection);
            Assert.Same(clientEntity, receiver2);

            config.PrefetchCount = 100;
            clientEntity = provider.CreateClientEntity("entityPath1", connection);
            Assert.Equal(100, ((QueueClient)clientEntity).PrefetchCount);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void CreateMessageSender_ReturnsExpectedSender(bool useManagedIdentity)
        {
            var connection = new ServiceBusConnection(useManagedIdentity ? defaultEndpoint : defaultConnection, useManagedIdentity);
            var config = new ServiceBusOptions
            {
                ConnectionString = defaultConnection
            };
            var provider = new MessagingProvider(new OptionsWrapper<ServiceBusOptions>(config));
            var sender = provider.CreateMessageSender("entityPath", connection);
            Assert.Equal("entityPath", sender.Path);

            var sender2 = provider.CreateMessageSender("entityPath", connection);
            Assert.Same(sender, sender2);
        }
    }
}

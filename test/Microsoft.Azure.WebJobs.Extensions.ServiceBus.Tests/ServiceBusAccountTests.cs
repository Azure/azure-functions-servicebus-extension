// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using Xunit;

namespace Microsoft.Azure.WebJobs.ServiceBus.UnitTests
{
    public class ServiceBusAccountTests
    {
        private readonly IConfiguration _configuration;
        private static string defaultConnectionString = "Endpoint=sb://default.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123=";
        private static string defaultEndpoint = "sb://default.servicebus.windows.net/";

        public ServiceBusAccountTests()
        {
            _configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new List<KeyValuePair<string, string>>
                {
                    new KeyValuePair<string, string>("sb-conn-str", defaultConnectionString)
                })
                .AddEnvironmentVariables()
                .Build();
        }

        [Fact]
        public void GetConnectionString_ReturnsExpectedConnectionString()
        {
            var options = new ServiceBusOptions()
            {
                ConnectionString = defaultConnectionString
            };
            var attribute = new ServiceBusTriggerAttribute("entity-name");
            var account = new ServiceBusAccount(options, _configuration, attribute);

            Assert.True(defaultConnectionString == account.Connection.ConnectionValue);
        }

        [Fact]
        public void GetMSIConnectionEndpoint_ReturnsExpectedConnectionEndpoint()
        {
            var options = new ServiceBusOptions()
            {
                Endpoint = defaultEndpoint,
                UseManagedServiceIdentity = true
            };
            var attribute = new ServiceBusTriggerAttribute("entity-name");
            var account = new ServiceBusAccount(options, _configuration, attribute);

            Assert.True(defaultEndpoint == account.Connection.ConnectionValue);
        }

        [Fact]
        public void GetConnectionStringFromAttribute_ReturnsExpectedConnectionString()
        {
            var options = new ServiceBusOptions()
            {
                Endpoint = defaultEndpoint,
                UseManagedServiceIdentity = true
            };
            var attribute = new ServiceBusTriggerAttribute("entity-name"){ Connection = "sb-conn-str"};
            var account = new ServiceBusAccount(options, _configuration, attribute);

            Assert.True(defaultConnectionString == account.Connection.ConnectionValue);
            Assert.False(account.Connection.IsManagedIdentityConnection);
        }

        [Fact]
        public void GetConnectionString_ThrowsIfConnectionStringNullOrEmpty()
        {
            var config = new ServiceBusOptions();
            var attribute = new ServiceBusTriggerAttribute("testqueue");
            attribute.Connection = "MissingConnection";

            var ex = Assert.Throws<InvalidOperationException>(() =>
            {
                var account = new ServiceBusAccount(config, _configuration, attribute);
                var cs = account.Connection.ConnectionValue;
            });
            Assert.Equal("Microsoft Azure WebJobs SDK ServiceBus connection string 'MissingConnection' is missing or empty.", ex.Message);

            attribute.Connection = null;
            config.ConnectionString = null;
            ex = Assert.Throws<InvalidOperationException>(() =>
            {
                var account = new ServiceBusAccount(config, _configuration, attribute);
                var cs = account.Connection.ConnectionValue;
            });
            Assert.Equal("Microsoft Azure WebJobs SDK ServiceBus connection string 'AzureWebJobsServiceBus' is missing or empty.", ex.Message);
        }

        [Fact]
        public void GetMSIConnectionEndpoint_ThrowsIfConnectionEndpointNullOrEmpty()
        {
            var config = new ServiceBusOptions()
            {
                UseManagedServiceIdentity = true
            };
            var attribute = new ServiceBusTriggerAttribute("testqueue");
            attribute.Connection = "MissingConnection";

            var ex = Assert.Throws<InvalidOperationException>(() =>
            {
                var account = new ServiceBusAccount(config, _configuration, attribute);
                var cs = account.Connection.ConnectionValue;
            });
            Assert.Equal("Microsoft Azure WebJobs SDK ServiceBus endpoint 'MissingConnection' is missing or empty.", ex.Message);

            attribute.Connection = null;
            config.ConnectionString = null;
            ex = Assert.Throws<InvalidOperationException>(() =>
            {
                var account = new ServiceBusAccount(config, _configuration, attribute);
                var cs = account.Connection.ConnectionValue;
            });
            Assert.Equal("Microsoft Azure WebJobs SDK ServiceBus endpoint 'AzureWebJobsServiceBus' is missing or empty.", ex.Message);
        }
    }
}

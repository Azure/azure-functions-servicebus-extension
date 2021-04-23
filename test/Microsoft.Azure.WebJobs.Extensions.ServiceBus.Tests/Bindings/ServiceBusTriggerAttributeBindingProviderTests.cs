// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs.Host.Triggers;
using Microsoft.Azure.WebJobs.ServiceBus.Triggers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;
using Microsoft.Azure.WebJobs.Host.Listeners;
using Microsoft.Azure.WebJobs.Host.Protocols;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs.Host.TestCommon;

namespace Microsoft.Azure.WebJobs.ServiceBus.UnitTests.Bindings
{
    public class ServiceBusTriggerAttributeBindingProviderTests
    {
        private readonly Mock<MessagingProvider> _mockMessagingProvider;
        private readonly ServiceBusTriggerAttributeBindingProvider _provider;
        private readonly IConfiguration _configuration;
        private readonly string _connectionString;
        private readonly ServiceBusOptions _options;

        public ServiceBusTriggerAttributeBindingProviderTests()
        {
            _configuration = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .AddTestSettings()
                .Build();

            // Add all test configuration to the environment as WebJobs requires a few of them to be in the environment
            foreach (var kv in _configuration.AsEnumerable())
            {
                Environment.SetEnvironmentVariable(kv.Key, kv.Value);
            }

            Mock<INameResolver> mockResolver = new Mock<INameResolver>(MockBehavior.Strict);
            _connectionString = _configuration.GetConnectionStringOrSetting(ServiceBus.Constants.DefaultConnectionStringName);

            _options = new ServiceBusOptions()
            {
                ConnectionString = _connectionString
            };
            _mockMessagingProvider = new Mock<MessagingProvider>(MockBehavior.Strict, new OptionsWrapper<ServiceBusOptions>(_options));

            Mock<IConverterManager> convertManager = new Mock<IConverterManager>(MockBehavior.Default);

            _provider = new ServiceBusTriggerAttributeBindingProvider(mockResolver.Object, _options, _mockMessagingProvider.Object, _configuration, NullLoggerFactory.Instance, convertManager.Object);
        }

        [Fact]
        public async Task TryCreateAsync_AccountOverride_OverrideIsApplied()
        {
            ParameterInfo parameter = GetType().GetMethod("TestJob_AccountOverride").GetParameters()[0];
            TriggerBindingProviderContext context = new TriggerBindingProviderContext(parameter, CancellationToken.None);

            ITriggerBinding binding = await _provider.TryCreateAsync(context);

            Assert.NotNull(binding);
        }

        [Fact]
        public async Task TryCreateAsync_DefaultAccount()
        {
            ParameterInfo parameter = GetType().GetMethod("TestJob").GetParameters()[0];
            TriggerBindingProviderContext context = new TriggerBindingProviderContext(parameter, CancellationToken.None);

            ITriggerBinding binding = await _provider.TryCreateAsync(context);

            Assert.NotNull(binding);
        }

        [Fact]
        public async Task GetServiceBusOptions_AutoCompleteDisabledOnTrigger()
        {
            var listenerContext = new ListenerFactoryContext(
                new Mock<FunctionDescriptor>().Object,
                new Mock<ITriggeredFunctionExecutor>().Object,
                CancellationToken.None);
            var parameters = new object[] { listenerContext };
            var entityPath = "autocomplete";

            var _mockMessageProcessor = new Mock<MessageProcessor>(MockBehavior.Strict, new MessageReceiver(_connectionString, entityPath), _options.MessageHandlerOptions);
            _mockMessagingProvider
                .Setup(p => p.CreateMessageProcessor(entityPath, _connectionString))
                .Returns(_mockMessageProcessor.Object);

            var parameter = GetType().GetMethod("TestAutoCompleteDisbledOnTrigger").GetParameters()[0];
            var context = new TriggerBindingProviderContext(parameter, CancellationToken.None);
            var binding = await _provider.TryCreateAsync(context);
            var createListenerTask = binding.GetType().GetMethod("CreateListenerAsync");
            var listener = await (Task<IListener>)createListenerTask.Invoke(binding, parameters);
            var listenerOptions = (ServiceBusOptions)listener.GetType().GetField("_serviceBusOptions", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(listener);

            Assert.NotNull(listenerOptions);
            Assert.True(_options.MessageHandlerOptions.AutoComplete);
            Assert.True(_options.SessionHandlerOptions.AutoComplete);
            Assert.True(_options.BatchOptions.AutoComplete);
            Assert.False(listenerOptions.MessageHandlerOptions.AutoComplete);
            Assert.False(listenerOptions.SessionHandlerOptions.AutoComplete);
            Assert.False(listenerOptions.BatchOptions.AutoComplete);
        }

        public static void TestJob_AccountOverride(
            [ServiceBusTriggerAttribute("test"),
             ServiceBusAccount(Constants.DefaultConnectionStringName)] Message message)
        {
            message = new Message();
        }

        public static void TestJob(
            [ServiceBusTriggerAttribute("test", Connection = Constants.DefaultConnectionStringName)] Message message)
        {
            message = new Message();
        }

        public static void TestAutoCompleteDisbledOnTrigger(
            [ServiceBusTriggerAttribute("autocomplete", AutoComplete = false),
             ServiceBusAccount(Constants.DefaultConnectionStringName)] Message message)
        {
            message = new Message();
        }
    }
}

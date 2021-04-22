// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Linq;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs.Host.TestCommon;
using Microsoft.Azure.WebJobs.Logging;
using Microsoft.Azure.WebJobs.ServiceBus.Config;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Microsoft.Azure.WebJobs.ServiceBus.UnitTests.Config
{
    public class ServiceBusOptionsTests
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly TestLoggerProvider _loggerProvider;

        public ServiceBusOptionsTests()
        {
            _loggerFactory = new LoggerFactory();
            _loggerProvider = new TestLoggerProvider();
            _loggerFactory.AddProvider(_loggerProvider);
        }

        [Fact]
        public void Constructor_SetsExpectedDefaults()
        {
            ServiceBusOptions config = new ServiceBusOptions();
            Assert.Equal(16 * Utility.GetProcessorCount(), config.MessageHandlerOptions.MaxConcurrentCalls);
            Assert.Equal(0, config.PrefetchCount);
        }

        [Fact]
        public void PrefetchCount_GetSet()
        {
            ServiceBusOptions config = new ServiceBusOptions();
            Assert.Equal(0, config.PrefetchCount);
            config.PrefetchCount = 100;
            Assert.Equal(100, config.PrefetchCount);
        }

        [Fact]
        public void LogExceptionReceivedEvent_NonTransientEvent_LoggedAsError()
        {
            var ex = new ServiceBusException(false);
            Assert.False(ex.IsTransient);
            ExceptionReceivedEventArgs e = new ExceptionReceivedEventArgs(ex, "TestAction", "TestEndpoint", "TestEntity", "TestClient");
            ServiceBusExtensionConfigProvider.LogExceptionReceivedEvent(e, _loggerFactory);

            var expectedMessage = $"Message processing error (Action=TestAction, ClientId=TestClient, EntityPath=TestEntity, Endpoint=TestEndpoint)";
            var logMessage = _loggerProvider.GetAllLogMessages().Single();
            Assert.Equal(LogLevel.Error, logMessage.Level);
            Assert.Same(ex, logMessage.Exception);
            Assert.Equal(expectedMessage, logMessage.FormattedMessage);
        }

        [Fact]
        public void LogExceptionReceivedEvent_TransientEvent_LoggedAsInformation()
        {
            var ex = new ServiceBusException(true);
            Assert.True(ex.IsTransient);
            ExceptionReceivedEventArgs e = new ExceptionReceivedEventArgs(ex, "TestAction", "TestEndpoint", "TestEntity", "TestClient");
            ServiceBusExtensionConfigProvider.LogExceptionReceivedEvent(e, _loggerFactory);

            var expectedMessage = $"Message processing error (Action=TestAction, ClientId=TestClient, EntityPath=TestEntity, Endpoint=TestEndpoint)";
            var logMessage = _loggerProvider.GetAllLogMessages().Single();
            Assert.Equal(LogLevel.Information, logMessage.Level);
            Assert.Same(ex, logMessage.Exception);
            Assert.Equal(expectedMessage, logMessage.FormattedMessage);
        }

        [Fact]
        public void LogExceptionReceivedEvent_NonMessagingException_LoggedAsError()
        {
            var ex = new MissingMethodException("What method??");
            ExceptionReceivedEventArgs e = new ExceptionReceivedEventArgs(ex, "TestAction", "TestEndpoint", "TestEntity", "TestClient");
            ServiceBusExtensionConfigProvider.LogExceptionReceivedEvent(e, _loggerFactory);

            var expectedMessage = $"Message processing error (Action=TestAction, ClientId=TestClient, EntityPath=TestEntity, Endpoint=TestEndpoint)";
            var logMessage = _loggerProvider.GetAllLogMessages().Single();
            Assert.Equal(LogLevel.Error, logMessage.Level);
            Assert.Same(ex, logMessage.Exception);
            Assert.Equal(expectedMessage, logMessage.FormattedMessage);
        }

        [Fact]
        public void DeepClone()
        {
            var options = new ServiceBusOptions();
            var clonedOptions = ServiceBusOptions.DeepClone(options);
            Assert.NotEqual(options, clonedOptions);
            Assert.Equal(options.PrefetchCount, clonedOptions.PrefetchCount);
            Assert.Equal(options.ExceptionHandler, clonedOptions.ExceptionHandler);
            Assert.Equal(options.MessageHandlerOptions.AutoComplete, clonedOptions.MessageHandlerOptions.AutoComplete);
            Assert.Equal(options.MessageHandlerOptions.MaxAutoRenewDuration.Ticks, clonedOptions.MessageHandlerOptions.MaxAutoRenewDuration.Ticks);
            Assert.Equal(options.MessageHandlerOptions.MaxConcurrentCalls, clonedOptions.MessageHandlerOptions.MaxConcurrentCalls);
            Assert.Equal(options.BatchOptions.AutoComplete, clonedOptions.BatchOptions.AutoComplete);
            Assert.Equal(options.BatchOptions.MaxMessageCount, clonedOptions.BatchOptions.MaxMessageCount);
            Assert.Equal(options.BatchOptions.OperationTimeout.Ticks, clonedOptions.BatchOptions.OperationTimeout.Ticks);
            Assert.Equal(options.SessionHandlerOptions.AutoComplete, clonedOptions.SessionHandlerOptions.AutoComplete);
            Assert.Equal(options.SessionHandlerOptions.MaxAutoRenewDuration.Ticks, clonedOptions.SessionHandlerOptions.MaxAutoRenewDuration.Ticks);
            Assert.Equal(options.SessionHandlerOptions.MaxConcurrentSessions, clonedOptions.SessionHandlerOptions.MaxConcurrentSessions);
            Assert.Equal(options.SessionHandlerOptions.MessageWaitTimeout.Ticks, clonedOptions.SessionHandlerOptions.MessageWaitTimeout.Ticks);

            // Perform updates on cloned options. They should not copied over to the source
            clonedOptions.MessageHandlerOptions.AutoComplete = options.BatchOptions.AutoComplete = options.SessionHandlerOptions.AutoComplete = false;
            Assert.NotEqual(options.MessageHandlerOptions.AutoComplete, clonedOptions.MessageHandlerOptions.AutoComplete);
            Assert.NotEqual(options.BatchOptions.AutoComplete, clonedOptions.BatchOptions.AutoComplete);
            Assert.NotEqual(options.SessionHandlerOptions.AutoComplete, clonedOptions.SessionHandlerOptions.AutoComplete);

            clonedOptions.MessageHandlerOptions.MaxAutoRenewDuration = new TimeSpan(0, 10, 0);
            Assert.NotEqual(options.MessageHandlerOptions.MaxAutoRenewDuration.Ticks, clonedOptions.MessageHandlerOptions.MaxAutoRenewDuration.Ticks);
        }
    }
}

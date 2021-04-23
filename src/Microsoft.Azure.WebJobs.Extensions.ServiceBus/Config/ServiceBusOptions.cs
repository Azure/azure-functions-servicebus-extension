// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs.Hosting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs.ServiceBus
{
    /// <summary>
    /// Configuration options for the ServiceBus extension.
    /// </summary>
    public class ServiceBusOptions : IOptionsFormatter
    {
        /// <summary>
        /// Constructs a new instance.
        /// </summary>
        public ServiceBusOptions()
        {
            // Our default options will delegate to our own exception
            // logger. Customers can override this completely by setting their
            // own MessageHandlerOptions instance.
            MessageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = Utility.GetProcessorCount() * 16
            };

            SessionHandlerOptions = new SessionHandlerOptions(ExceptionReceivedHandler);

            // Default operation timeout is 1 minute in ServiceBus SDK
            // https://github.com/Azure/azure-sdk-for-net/blob/master/sdk/servicebus/Microsoft.Azure.ServiceBus/src/Constants.cs#L30
            BatchOptions = new BatchOptions()
            {
                MaxMessageCount = 1000,
                OperationTimeout = TimeSpan.FromMinutes(1),
                AutoComplete = true
            };
        }

        /// <summary>
        /// Gets or sets the Azure ServiceBus connection string.
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the default <see cref="Azure.ServiceBus.MessageHandlerOptions"/> that will be used by
        /// <see cref="MessageReceiver"/>s.
        /// </summary>
        public MessageHandlerOptions MessageHandlerOptions { get; set; }

        /// <summary>
        /// Gets or sets the default <see cref="Azure.ServiceBus.SessionHandlerOptions"/> that will be used by
        /// <see cref="ClientEntity"/>s.
        /// </summary>
        public SessionHandlerOptions SessionHandlerOptions { get; set; }

        /// <summary>
        /// Gets or sets the default PrefetchCount that will be used by <see cref="MessageReceiver"/>s.
        /// </summary>
        public int PrefetchCount { get; set; }

        /// <summary>
        /// Gets or sets the default <see cref="Azure.ServiceBus.BatchOptions"/> that will be used by
        /// <see cref="ClientEntity"/>s.
        /// </summary>
        public BatchOptions BatchOptions { get; set; }

        internal Action<ExceptionReceivedEventArgs> ExceptionHandler { get; set; }

        public string Format()
        {
            JObject messageHandlerOptions = null;
            if (MessageHandlerOptions != null)
            {
                messageHandlerOptions = new JObject
                {
                    { nameof(MessageHandlerOptions.AutoComplete), MessageHandlerOptions.AutoComplete },
                    { nameof(MessageHandlerOptions.MaxAutoRenewDuration), MessageHandlerOptions.MaxAutoRenewDuration },
                    { nameof(MessageHandlerOptions.MaxConcurrentCalls), MessageHandlerOptions.MaxConcurrentCalls }
                };
            }

            JObject sessionHandlerOptions = null;
            if (SessionHandlerOptions != null)
            {
                sessionHandlerOptions = new JObject
                {
                    { nameof(SessionHandlerOptions.AutoComplete), SessionHandlerOptions.AutoComplete },
                    { nameof(SessionHandlerOptions.MaxAutoRenewDuration), SessionHandlerOptions.MaxAutoRenewDuration },
                    { nameof(SessionHandlerOptions.MaxConcurrentSessions), SessionHandlerOptions.MaxConcurrentSessions },
                    { nameof(SessionHandlerOptions.MessageWaitTimeout), SessionHandlerOptions.MessageWaitTimeout }
                };
            }

            JObject batchOptions = null;
            if (BatchOptions != null)
            {
                batchOptions = new JObject
                {
                    { nameof(BatchOptions.MaxMessageCount), BatchOptions.MaxMessageCount },
                    { nameof(BatchOptions.OperationTimeout), BatchOptions.OperationTimeout },
                    { nameof(BatchOptions.AutoComplete), BatchOptions.AutoComplete },
                };
            }

            // Do not include ConnectionString in loggable options.
            JObject options = new JObject
            {
                { nameof(PrefetchCount), PrefetchCount },
                { nameof(MessageHandlerOptions), messageHandlerOptions },
                { nameof(SessionHandlerOptions), sessionHandlerOptions },
                { nameof(BatchOptions), batchOptions}
            };

            return options.ToString(Formatting.Indented);
        }

        /// <summary>
        /// Deep clones service bus options.
        /// </summary>
        /// <param name="source">The soure service bus options.</param>
        internal static ServiceBusOptions DeepClone(ServiceBusOptions source)
        {
            if (source == null)
            {
                return null;
            }

            var options = new ServiceBusOptions()
            {
                ConnectionString = source.ConnectionString,
                PrefetchCount = source.PrefetchCount,
            };

            options.MessageHandlerOptions.AutoComplete = source.MessageHandlerOptions.AutoComplete;
            options.MessageHandlerOptions.MaxAutoRenewDuration = new TimeSpan(source.MessageHandlerOptions.MaxAutoRenewDuration.Ticks);
            options.MessageHandlerOptions.MaxConcurrentCalls = source.MessageHandlerOptions.MaxConcurrentCalls;

            options.SessionHandlerOptions.AutoComplete = source.SessionHandlerOptions.AutoComplete;
            options.SessionHandlerOptions.MaxConcurrentSessions = source.SessionHandlerOptions.MaxConcurrentSessions;
            options.SessionHandlerOptions.MaxAutoRenewDuration = new TimeSpan(source.SessionHandlerOptions.MaxAutoRenewDuration.Ticks);
            options.SessionHandlerOptions.MessageWaitTimeout = new TimeSpan(source.SessionHandlerOptions.MessageWaitTimeout.Ticks);

            options.BatchOptions.AutoComplete = source.BatchOptions.AutoComplete;
            options.BatchOptions.MaxMessageCount = source.BatchOptions.MaxMessageCount;
            options.BatchOptions.OperationTimeout = new TimeSpan(source.BatchOptions.OperationTimeout.Ticks);

            return options;
        }

        private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs args)
        {
            ExceptionHandler?.Invoke(args);

            return Task.CompletedTask;
        }
    }
}

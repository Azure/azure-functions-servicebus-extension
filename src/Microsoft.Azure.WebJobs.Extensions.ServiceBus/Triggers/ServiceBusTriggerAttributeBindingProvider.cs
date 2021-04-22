// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Globalization;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Host.Bindings;
using Microsoft.Azure.WebJobs.Host.Listeners;
using Microsoft.Azure.WebJobs.Host.Protocols;
using Microsoft.Azure.WebJobs.Host.Triggers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.WebJobs.ServiceBus.Listeners;
using Microsoft.Azure.WebJobs.Host.Config;

namespace Microsoft.Azure.WebJobs.ServiceBus.Triggers
{
    internal class ServiceBusTriggerAttributeBindingProvider : ITriggerBindingProvider
    {

        private readonly INameResolver _nameResolver;
        private readonly ServiceBusOptions _options;
        private readonly MessagingProvider _messagingProvider;
        private readonly IConfiguration _configuration;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IConverterManager _converterManager;
        private readonly ILogger<ServiceBusTriggerAttributeBindingProvider> _logger;

        public ServiceBusTriggerAttributeBindingProvider(INameResolver nameResolver, ServiceBusOptions options, MessagingProvider messagingProvider, IConfiguration configuration,
            ILoggerFactory loggerFactory, IConverterManager converterManager)
        {
            _nameResolver = nameResolver ?? throw new ArgumentNullException(nameof(nameResolver));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _messagingProvider = messagingProvider ?? throw new ArgumentNullException(nameof(messagingProvider));
            _configuration = configuration;
            _loggerFactory = loggerFactory;
            _converterManager = converterManager;
            _logger = _loggerFactory.CreateLogger<ServiceBusTriggerAttributeBindingProvider>();
        }

        public Task<ITriggerBinding> TryCreateAsync(TriggerBindingProviderContext context)
        {
            if (context == null)
            {
                throw new ArgumentNullException("context");
            }

            ParameterInfo parameter = context.Parameter;
            var attribute = TypeUtility.GetResolvedAttribute<ServiceBusTriggerAttribute>(parameter);

            if (attribute == null)
            {
                return Task.FromResult<ITriggerBinding>(null);
            }

            string queueName = null;
            string topicName = null;
            string subscriptionName = null;
            string entityPath = null;
            EntityType entityType;

            if (attribute.QueueName != null)
            {
                queueName = Resolve(attribute.QueueName);
                entityPath = queueName;
                entityType = EntityType.Queue;
            }
            else
            {
                topicName = Resolve(attribute.TopicName);
                subscriptionName = Resolve(attribute.SubscriptionName);
                entityPath = EntityNameHelper.FormatSubscriptionPath(topicName, subscriptionName);
                entityType = EntityType.Topic;
            }

            attribute.Connection = Resolve(attribute.Connection);
            ServiceBusAccount account = new ServiceBusAccount(_options, _configuration, attribute);

            Func<ListenerFactoryContext, bool, Task<IListener>> createListener =
            (factoryContext, singleDispatch) =>
            {
                var options = GetServiceBusOptions(attribute, factoryContext.Descriptor.ShortName);

                IListener listener = new ServiceBusListener(factoryContext.Descriptor.Id, entityType, entityPath, attribute.IsSessionsEnabled, factoryContext.Executor, options, account, _messagingProvider, _loggerFactory, singleDispatch);
                return Task.FromResult(listener);
            };

            ITriggerBinding binding = BindingFactory.GetTriggerBinding(new ServiceBusTriggerBindingStrategy(), parameter, _converterManager, createListener);

            return Task.FromResult<ITriggerBinding>(binding);
        }

        private string Resolve(string queueName)
        {
            if (_nameResolver == null)
            {
                return queueName;
            }

            return _nameResolver.ResolveWholeString(queueName);
        }

        /// <summary>
        /// Gets service bus options after applying function level options if needed.
        /// </summary>
        /// <param name="attribute">The trigger attribute.</param>
        /// <param name="functionName">The function name.</param>
        private ServiceBusOptions GetServiceBusOptions(ServiceBusTriggerAttribute attribute, string functionName)
        {
            if (attribute.IsAutoCompleteOptionSet)
            {
                var options = ServiceBusOptions.DeepClone(_options);

                _logger.LogInformation($"The 'AutoComplete' option has been overrriden to '{attribute.AutoComplete}' value for '{functionName}' function.");

                options.BatchOptions.AutoComplete = options.MessageHandlerOptions.AutoComplete = options.SessionHandlerOptions.AutoComplete = attribute.AutoComplete;

                return options;
            }

            return _options;
        }
    }
}

// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Host.Scale;

namespace Microsoft.Azure.WebJobs.ServiceBus.Listeners
{
    public class ScaleControllerMonitorFactory : IScaleMonitorProviderForScaleController
    {
        public IScaleMonitor Create(ScaleMonitorContext context)
        {
            var serviceBusOption = context.GetExtensionOption<ServiceBusOptions>("serviceBus");
            var attribute = context.GetTriggerAttribute<ServiceBusTriggerAttribute>();
            var messageProvider = new MessagingProvider(serviceBusOption);

            string entityPath = null;
            string queueName = null;
            string topicName = null;
            string subscriptionName = null;
            EntityType entityType;
            if (attribute.QueueName != null)
            {
                queueName = Resolve(context.NameResolver, attribute.QueueName);
                entityPath = queueName;
                entityType = EntityType.Queue;
            } else
            {
                topicName = Resolve(context.NameResolver, attribute.TopicName);
                subscriptionName = Resolve(context.NameResolver, attribute.SubscriptionName);
                entityPath = EntityNameHelper.FormatSubscriptionPath(topicName, subscriptionName);
                entityType = EntityType.Topic;
            }
            string connectionString = context.NameResolver.Resolve(attribute.Connection);
            var messageReceiver = messageProvider.CreateMessageReceiver(entityPath, connectionString);
            return new ServiceBusScaleMonitor(context.FunctionId, entityType, entityPath, connectionString, new Lazy<MessageReceiver>(() => messageReceiver), context.LoggerFactory);
        }

        private string Resolve(INameResolver resolver, string queueName)
        {
            if (resolver == null)
            {
                return queueName;
            }

            return resolver.ResolveWholeString(queueName);
        }
    }
}

// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.Primitives;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;

namespace Microsoft.Azure.WebJobs.ServiceBus
{
    /// <summary>
    /// This class provides factory methods for the creation of instances
    /// used for ServiceBus message processing.
    /// </summary>
    public class MessagingProvider
    {
        private readonly ServiceBusOptions _options;
        private readonly TokenProvider _tokenProvider;
        private readonly ConcurrentDictionary<string, MessageReceiver> _messageReceiverCache = new ConcurrentDictionary<string, MessageReceiver>();
        private readonly ConcurrentDictionary<string, MessageSender> _messageSenderCache = new ConcurrentDictionary<string, MessageSender>();
        private readonly ConcurrentDictionary<string, ClientEntity> _clientEntityCache = new ConcurrentDictionary<string, ClientEntity>();

        /// <summary>
        /// Constructs a new instance.
        /// </summary>
        /// <param name="serviceBusOptions">The <see cref="ServiceBusOptions"/>.</param>
        public MessagingProvider(IOptions<ServiceBusOptions> serviceBusOptions)
        {
            _options = serviceBusOptions?.Value ?? throw new ArgumentNullException(nameof(serviceBusOptions));
            if (_options.UseManagedServiceIdentity && string.IsNullOrEmpty(_options.Endpoint))
            {
                throw new ArgumentNullException(nameof(_options.Endpoint));
            }
            _tokenProvider = _options.ServiceBusTokenProvider ?? TokenProvider.CreateManagedServiceIdentityTokenProvider();
        }

        /// <summary>
        /// Creates a <see cref="MessageProcessor"/> for the specified ServiceBus entity.
        /// </summary>
        /// <param name="entityPath">The ServiceBus entity to create a <see cref="MessageProcessor"/> for.</param>
        /// <param name="connection">The ServiceBus connection connection.</param>
        /// <returns>The <see cref="MessageProcessor"/>.</returns>
        public virtual MessageProcessor CreateMessageProcessor(string entityPath, ServiceBusConnection connection)
        {
            if (string.IsNullOrEmpty(entityPath))
            {
                throw new ArgumentNullException("entityPath");
            }
            if (connection == null)
            {
                throw new ArgumentNullException("connection");
            }

            return new MessageProcessor(GetOrAddMessageReceiver(entityPath, connection), _options.MessageHandlerOptions);
        }

        /// <summary>
        /// Creates a <see cref="MessageReceiver"/> for the specified ServiceBus entity.
        /// </summary>
        /// <remarks>
        /// You can override this method to customize the <see cref="MessageReceiver"/>.
        /// </remarks>
        /// <param name="entityPath">The ServiceBus entity to create a <see cref="MessageReceiver"/> for.</param>
        /// <param name="connection">The ServiceBus connection connection.</param>
        /// <returns></returns>
        public virtual MessageReceiver CreateMessageReceiver(string entityPath, ServiceBusConnection connection)
        {
            if (string.IsNullOrEmpty(entityPath))
            {
                throw new ArgumentNullException("entityPath");
            }
            if (connection == null)
            {
                throw new ArgumentNullException("connection");
            }

            return GetOrAddMessageReceiver(entityPath, connection);
        }

        /// <summary>
        /// Creates a <see cref="MessageSender"/> for the specified ServiceBus entity.
        /// </summary>
        /// <remarks>
        /// You can override this method to customize the <see cref="MessageSender"/>.
        /// </remarks>
        /// <param name="entityPath">The ServiceBus entity to create a <see cref="MessageSender"/> for.</param>
        /// <param name="connection">The ServiceBus connection connection.</param>
        /// <returns></returns>
        public virtual MessageSender CreateMessageSender(string entityPath, ServiceBusConnection connection)
        {
            if (string.IsNullOrEmpty(entityPath))
            {
                throw new ArgumentNullException("entityPath");
            }
            if (connection == null)
            {
                throw new ArgumentNullException("connection");
            }

            return GetOrAddMessageSender(entityPath, connection);
        }

        /// <summary>
        /// Creates a <see cref="ClientEntity"/> for the specified ServiceBus entity.
        /// </summary>
        /// <remarks>
        /// You can override this method to customize the <see cref="ClientEntity"/>.
        /// </remarks>
        /// <param name="entityPath">The ServiceBus entity to create a <see cref="ClientEntity"/> for.</param>
        /// <param name="connection">The ServiceBus connection connection.</param>
        /// <returns></returns>
        public virtual ClientEntity CreateClientEntity(string entityPath, ServiceBusConnection connection)
        {
            if (string.IsNullOrEmpty(entityPath))
            {
                throw new ArgumentNullException("entityPath");
            }
            if (connection == null)
            {
                throw new ArgumentNullException("connection");
            }

            return GetOrAddClientEntity(entityPath, connection);
        }

        /// <summary>
        /// Creates a <see cref="SessionMessageProcessor"/> for the specified ServiceBus entity.
        /// </summary>
        /// <param name="entityPath">The ServiceBus entity to create a <see cref="SessionMessageProcessor"/> for.</param>
        /// <param name="connection">The ServiceBus connection connection.</param>
        /// <returns></returns>
        public virtual SessionMessageProcessor CreateSessionMessageProcessor(string entityPath, ServiceBusConnection connection)
        {
            if (string.IsNullOrEmpty(entityPath))
            {
                throw new ArgumentNullException("entityPath");
            }
            if (connection == null)
            {
                throw new ArgumentNullException("connection");
            }

            return new SessionMessageProcessor(GetOrAddClientEntity(entityPath, connection), _options.SessionHandlerOptions);
        }

        private MessageReceiver GetOrAddMessageReceiver(string entityPath, ServiceBusConnection connection)
        {
            var cacheKey = $"{entityPath}-{connection.ConnectionValue}";
            if (connection.IsManagedIdentityConnection)
            {
                return _messageReceiverCache.GetOrAdd(cacheKey,
                    new MessageReceiver(connection.ConnectionValue, entityPath, _tokenProvider)
                    {
                        PrefetchCount = _options.PrefetchCount
                    });
            }
            else
            {
                return _messageReceiverCache.GetOrAdd(cacheKey,
                    new MessageReceiver(connection.ConnectionValue, entityPath)
                    {
                        PrefetchCount = _options.PrefetchCount
                    });
            }
        }

        private MessageSender GetOrAddMessageSender(string entityPath, ServiceBusConnection connection)
        {
            var cacheKey = $"{entityPath}-{connection.ConnectionValue}";
            if (connection.IsManagedIdentityConnection)
            {
                return _messageSenderCache.GetOrAdd(cacheKey, new MessageSender(connection.ConnectionValue, entityPath, _tokenProvider));
            }
            else
            {
                return _messageSenderCache.GetOrAdd(cacheKey, new MessageSender(connection.ConnectionValue, entityPath));
            }
        }

        private ClientEntity GetOrAddClientEntity(string entityPath, ServiceBusConnection connection)
        {
            var cacheKey = $"{entityPath}-{connection.ConnectionValue}";
            string[] arr = entityPath.Split(new string[] { "/Subscriptions/" }, StringSplitOptions.None);
            if (arr.Length == 2)
            {
                // entityPath for a subscription is "{TopicName}/Subscriptions/{SubscriptionName}"
                if (connection.IsManagedIdentityConnection)
                {
                    return _clientEntityCache.GetOrAdd(cacheKey, new SubscriptionClient(connection.ConnectionValue, arr[0], arr[1], _tokenProvider)
                    {
                        PrefetchCount = _options.PrefetchCount
                    });
                }
                else
                {
                    return _clientEntityCache.GetOrAdd(cacheKey, new SubscriptionClient(connection.ConnectionValue, arr[0], arr[1])
                    {
                        PrefetchCount = _options.PrefetchCount
                    });
                }
            }
            else
            {
                // entityPath for a queue is "{QueueName}"
                if (connection.IsManagedIdentityConnection)
                {
                    return _clientEntityCache.GetOrAdd(cacheKey, new QueueClient(connection.ConnectionValue, entityPath, _tokenProvider)
                    {
                        PrefetchCount = _options.PrefetchCount
                    });
                }
                else
                {
                    return _clientEntityCache.GetOrAdd(cacheKey, new QueueClient(connection.ConnectionValue, entityPath)
                    {
                        PrefetchCount = _options.PrefetchCount
                    });
                }
            }
        }
    }
}
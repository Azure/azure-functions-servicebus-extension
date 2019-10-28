// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Listeners;
using Microsoft.Azure.WebJobs.Host.Scale;
using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.WebJobs.ServiceBus.Listeners
{
    internal sealed class ServiceBusListener : IListener, IScaleMonitorProvider
    {
        private readonly MessagingProvider _messagingProvider;
        private readonly string _functionId;
        private readonly EntityType _entityType;
        private readonly string _entityPath;
        private readonly bool _isSessionsEnabled;
        private readonly ServiceBusTriggerExecutor _triggerExecutor;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly MessageProcessor _messageProcessor;
        private readonly ServiceBusAccount _serviceBusAccount;
        private readonly ServiceBusOptions _serviceBusOptions;
        private readonly ILoggerFactory _loggerFactory;

        private Lazy<MessageReceiver> _receiver;
        private ClientEntity _clientEntity;
        private bool _disposed;
        private bool _started;

        private IMessageSession _messageSession;
        private SessionMessageProcessor _sessionMessageProcessor;

        private Lazy<ServiceBusScaleMonitor> _scaleMonitor;

        public ServiceBusListener(string functionId, EntityType entityType, string entityPath, bool isSessionsEnabled, ServiceBusTriggerExecutor triggerExecutor,
            ServiceBusOptions config, ServiceBusAccount serviceBusAccount, MessagingProvider messagingProvider, ILoggerFactory loggerFactory)
        {
            _functionId = functionId;
            _entityType = entityType;
            _entityPath = entityPath;
            _isSessionsEnabled = isSessionsEnabled;
            _triggerExecutor = triggerExecutor;
            _cancellationTokenSource = new CancellationTokenSource();
            _messagingProvider = messagingProvider;
            _serviceBusAccount = serviceBusAccount;
            _loggerFactory = loggerFactory;
            _receiver = CreateMessageReceiver();
            _scaleMonitor = new Lazy<ServiceBusScaleMonitor>(() => new ServiceBusScaleMonitor(_functionId, _entityType, _entityPath, _serviceBusAccount.ConnectionString, _receiver, _loggerFactory));

            if (_isSessionsEnabled)
            {
                _sessionMessageProcessor = _messagingProvider.CreateSessionMessageProcessor(_entityPath, _serviceBusAccount.ConnectionString);
            }
            else
            {
                _messageProcessor = _messagingProvider.CreateMessageProcessor(entityPath, _serviceBusAccount.ConnectionString);
            }
            _serviceBusOptions = config;
        }

        internal MessageReceiver Receiver => _receiver.Value;

        internal IMessageSession MessageSession => _messageSession;

        public Task StartAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            if (_started)
            {
                throw new InvalidOperationException("The listener has already been started.");
            }

            if (_isSessionsEnabled)
            {
                _clientEntity = _messagingProvider.CreateClientEntity(_entityPath, _serviceBusAccount.ConnectionString);

                if (_clientEntity is QueueClient queueClient)
                {
                    queueClient.RegisterSessionHandler(ProcessSessionMessageAsync, _serviceBusOptions.SessionHandlerOptions);
                }
                else
                {
                    SubscriptionClient subscriptionClient = _clientEntity as SubscriptionClient;
                    subscriptionClient.RegisterSessionHandler(ProcessSessionMessageAsync, _serviceBusOptions.SessionHandlerOptions);
                }
            }
            else
            {
                Receiver.RegisterMessageHandler(ProcessMessageAsync, _serviceBusOptions.MessageHandlerOptions);
            }
            _started = true;

            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            if (!_started)
            {
                throw new InvalidOperationException("The listener has not yet been started or has already been stopped.");
            }

            // cancel our token source to signal any in progress
            // ProcessMessageAsync invocations to cancel
            _cancellationTokenSource.Cancel();

            if (_receiver != null && _receiver.IsValueCreated)
            {
                await Receiver.CloseAsync();
                _receiver = CreateMessageReceiver();
            }
            if (_clientEntity != null)
            {
                await _clientEntity.CloseAsync();
                _clientEntity = null;
            }
            _started = false;
        }

        public void Cancel()
        {
            ThrowIfDisposed();
            _cancellationTokenSource.Cancel();
        }

        [SuppressMessage("Microsoft.Usage", "CA2213:DisposableFieldsShouldBeDisposed", MessageId = "_cancellationTokenSource")]
        public void Dispose()
        {
            if (!_disposed)
            {
                // Running callers might still be using the cancellation token.
                // Mark it canceled but don't dispose of the source while the callers are running.
                // Otherwise, callers would receive ObjectDisposedException when calling token.Register.
                // For now, rely on finalization to clean up _cancellationTokenSource's wait handle (if allocated).
                _cancellationTokenSource.Cancel();

                if (_receiver != null && _receiver.IsValueCreated)
                {
                    Receiver.CloseAsync().Wait();
                    _receiver = null;
                }

                if (_clientEntity != null)
                {
                    _clientEntity.CloseAsync().Wait();
                    _clientEntity = null;
                }

                _disposed = true;
            }
        }

        private Lazy<MessageReceiver> CreateMessageReceiver()
        {
            return new Lazy<MessageReceiver>(() => _messagingProvider.CreateMessageReceiver(_entityPath, _serviceBusAccount.ConnectionString));
        }

        internal async Task ProcessMessageAsync(Message message, CancellationToken cancellationToken)
        {
            if (!await _messageProcessor.BeginProcessingMessageAsync(message, cancellationToken))
            {
                return;
            }

            FunctionResult result = await _triggerExecutor.ExecuteAsync(message, cancellationToken);
            await _messageProcessor.CompleteProcessingMessageAsync(message, result, cancellationToken);
        }

        internal async Task ProcessSessionMessageAsync(IMessageSession session, Message message, CancellationToken cancellationToken)
        {
            _messageSession = session;
            if (!await _sessionMessageProcessor.BeginProcessingMessageAsync(session, message, cancellationToken))
            {
                return;
            }

            FunctionResult result = await _triggerExecutor.ExecuteAsync(message, cancellationToken);
            await _sessionMessageProcessor.CompleteProcessingMessageAsync(session, message, result, cancellationToken);
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(null);
            }
        }

        public IScaleMonitor GetMonitor()
        {
            return _scaleMonitor.Value;
        }
    }
}

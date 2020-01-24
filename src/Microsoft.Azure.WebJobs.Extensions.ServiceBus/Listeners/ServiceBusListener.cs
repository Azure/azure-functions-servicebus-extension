// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Listeners;
using Microsoft.Azure.WebJobs.Host.Scale;
using Microsoft.Extensions.Logging;


namespace Microsoft.Azure.WebJobs.ServiceBus.Listeners
{
    internal sealed class ServiceBusListener : IListener, IScaleMonitorProvider
    {
        private const int NumberOfMessagesToReceiveInBatch = 1000;

        private readonly MessagingProvider _messagingProvider;
        private readonly ITriggeredFunctionExecutor _triggerExecutor;
        private readonly string _functionId;
        private readonly EntityType _entityType;
        private readonly string _entityPath;
        private readonly bool _isSessionsEnabled;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly MessageProcessor _messageProcessor;
        private readonly ServiceBusAccount _serviceBusAccount;
        private readonly ServiceBusOptions _serviceBusOptions;
        private readonly ILoggerFactory _loggerFactory;
        private readonly bool _singleDispatch;
        private readonly ILogger<ServiceBusListener> _logger;

        private Lazy<MessageReceiver> _receiver;
        private Lazy<SessionClient> _sessionClient;
        private ClientEntity _clientEntity;
        private bool _disposed;
        private bool _started;

        private IMessageSession _messageSession;
        private SessionMessageProcessor _sessionMessageProcessor;

        private Lazy<ServiceBusScaleMonitor> _scaleMonitor;

        public ServiceBusListener(string functionId, EntityType entityType, string entityPath, bool isSessionsEnabled, ITriggeredFunctionExecutor triggerExecutor,
            ServiceBusOptions config, ServiceBusAccount serviceBusAccount, MessagingProvider messagingProvider, ILoggerFactory loggerFactory, bool singleDispatch)
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
            _logger = loggerFactory.CreateLogger<ServiceBusListener>();
            _receiver = CreateMessageReceiver();
            _sessionClient = CreateSessionClient();
            _scaleMonitor = new Lazy<ServiceBusScaleMonitor>(() => new ServiceBusScaleMonitor(_functionId, _entityType, _entityPath, _serviceBusAccount.ConnectionString, _receiver, _loggerFactory));
            _singleDispatch = singleDispatch;

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

            if (_singleDispatch)
            {
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
            }
            else
            {
                StartMessageBatchReceiver(_cancellationTokenSource.Token);
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

                if (_sessionClient != null && _sessionClient.IsValueCreated)
                {
                    _sessionClient.Value.CloseAsync().Wait();
                    _sessionClient = null;
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

        private Lazy<SessionClient> CreateSessionClient()
        {
            return new Lazy<SessionClient>(() => _messagingProvider.CreateSessionClient(_entityPath, _serviceBusAccount.ConnectionString));
        }

        internal async Task ProcessMessageAsync(Message message, CancellationToken cancellationToken)
        {
            CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cancellationTokenSource.Token);

            if (!await _messageProcessor.BeginProcessingMessageAsync(message, linkedCts.Token))
            {
                return;
            }

            ServiceBusTriggerInput input = ServiceBusTriggerInput.CreateSingle(message);
            input.MessageReceiver = Receiver;

            TriggeredFunctionData data = input.GetTriggerFunctionData();
            FunctionResult result = await _triggerExecutor.TryExecuteAsync(data, linkedCts.Token);
            await _messageProcessor.CompleteProcessingMessageAsync(message, result, linkedCts.Token);
        }

        internal async Task ProcessSessionMessageAsync(IMessageSession session, Message message, CancellationToken cancellationToken)
        {
            CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cancellationTokenSource.Token);

            _messageSession = session;
            if (!await _sessionMessageProcessor.BeginProcessingMessageAsync(session, message, linkedCts.Token))
            {
                return;
            }

            ServiceBusTriggerInput input = ServiceBusTriggerInput.CreateSingle(message);
            input.MessageReceiver = session;

            TriggeredFunctionData data = input.GetTriggerFunctionData();
            FunctionResult result = await _triggerExecutor.TryExecuteAsync(data, linkedCts.Token);
            await _sessionMessageProcessor.CompleteProcessingMessageAsync(session, message, result, linkedCts.Token);
        }

        internal void StartMessageBatchReceiver(CancellationToken cancellationToken)
        {
            SessionClient sessionClient = null;
            IMessageReceiver receiver = null;
            if (_isSessionsEnabled)
            {
                sessionClient = _sessionClient.Value;
            }
            else
            {
                receiver = Receiver;
            }

            Task.Run(async () =>
            {
                while (true)
                {
                    try
                    {
                        if (!_started || cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }

                        if (_isSessionsEnabled)
                        {
                            try
                            {
                                receiver = await sessionClient.AcceptMessageSessionAsync();
                            }
                            catch (ServiceBusTimeoutException)
                            {
                                // it's expected if the entity is empty, try next time
                                continue;
                            }
                        }

                        IList<Message> messages = await receiver.ReceiveAsync(_serviceBusOptions.BatchOptions.MaxMessageCount, _serviceBusOptions.BatchOptions.OperationTimeout);

                        if (messages != null)
                        {
                            Message[] messagesArray = messages.ToArray();
                            ServiceBusTriggerInput input = ServiceBusTriggerInput.CreateBatch(messagesArray);
                            input.MessageReceiver = receiver;

                            FunctionResult result = await _triggerExecutor.TryExecuteAsync(input.GetTriggerFunctionData(), cancellationToken);

                            if (cancellationToken.IsCancellationRequested)
                            {
                                return;
                            }

                            // Complete batch of messages only if the execution was successful
                            if (_serviceBusOptions.BatchOptions.AutoComplete && _started)
                            {
                                if (result.Succeeded)
                                {
                                    await receiver.CompleteAsync(messagesArray.Select(x => x.SystemProperties.LockToken));
                                }
                                else
                                {
                                    // Delivery count is not incremented if 
                                    // Session is accepted, the messages within the session are not completed (even if they are locked), and the session is closed
                                    // https://docs.microsoft.com/en-us/azure/service-bus-messaging/message-sessions#impact-of-delivery-count
                                    if (_isSessionsEnabled)
                                    {
                                        List<Task> abandonTasks = new List<Task>();
                                        foreach (var lockTocken in messagesArray.Select(x => x.SystemProperties.LockToken))
                                        {
                                            abandonTasks.Add(receiver.AbandonAsync(lockTocken));
                                        }
                                        await Task.WhenAll(abandonTasks);
                                    }
                                }
                            }

                            // Close the session and release the session lock
                            if (_isSessionsEnabled)
                            {
                                await receiver.CloseAsync();
                            }
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                        // Ignore as we are stopping the host
                    }
                    catch (Exception ex)
                    {
                        // Log another exception
                        _logger.LogError($"An unhandled exception occurred in the message batch receive loop: {ex.ToString()}");
                    }
                }
            }, cancellationToken);
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

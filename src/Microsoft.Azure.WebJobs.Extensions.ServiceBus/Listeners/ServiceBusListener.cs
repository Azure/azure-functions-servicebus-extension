﻿// Copyright (c) .NET Foundation. All rights reserved.
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
        // Serialize execution of StopAsync to avoid calling Unregister* concurrently
        private readonly SemaphoreSlim _stopAsyncSemaphore = new SemaphoreSlim(1, 1);

        private IMessageSession _messageSession;
        private SessionMessageProcessor _sessionMessageProcessor;

        private Lazy<ServiceBusScaleMonitor> _scaleMonitor;
        private Lazy<string> _details;

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

            _details = new Lazy<string>(() =>
            {
                string endpoint = String.Empty;
                if (_clientEntity != null)
                {
                    endpoint = _clientEntity.ServiceBusConnection.Endpoint.ToString();
                } 
                else if (_receiver.IsValueCreated)
                {
                    endpoint = _receiver.Value.ServiceBusConnection.Endpoint.ToString();
                } 
                else if (_sessionClient.IsValueCreated)
                {
                    endpoint = _sessionClient.Value.ServiceBusConnection.Endpoint.ToString();
                }

                return $"namespace='{endpoint}', entityPath='{_entityPath}', singleDispatch='{_singleDispatch}', " +
                            $"isSessionsEnabled='{_isSessionsEnabled}', functionId='{_functionId}'";
            });
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

            // The _started flag is set here and not after StartMessageBatchReceiver is called
            // to avoid a race condition in which the batch receive loop could start before the _started
            // flag is set to true, leading to the listener never receiving any messages 
            _started = true;

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

            _logger.LogDebug($"ServiceBus listener started ({_details.Value})");
            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            await _stopAsyncSemaphore.WaitAsync();
            {
                try
                {
                    if (_started)
                    {
                        // Unregister* methods stop new messages from being processed while allowing in-flight messages to complete.
                        // As the amount of time functions are allowed to complete processing varies by SKU, we specify max timespan
                        // as the amount of time Service Bus SDK should wait for in-flight messages to complete procesing after 
                        // unregistering the message handler so that functions have as long as the host continues to run time to complete.
                        if (_singleDispatch)
                        {
                            if (_isSessionsEnabled)
                            {
                                if (_clientEntity != null)
                                {
                                    if (_clientEntity is QueueClient queueClient)
                                    {
                                        await queueClient.UnregisterSessionHandlerAsync(TimeSpan.MaxValue);
                                    }
                                    else
                                    {
                                        SubscriptionClient subscriptionClient = _clientEntity as SubscriptionClient;
                                        await subscriptionClient.UnregisterSessionHandlerAsync(TimeSpan.MaxValue);
                                    }
                                }
                            }
                            else
                            {
                                if (_receiver != null && _receiver.IsValueCreated)
                                {
                                    await Receiver.UnregisterMessageHandlerAsync(TimeSpan.MaxValue);
                                }
                            }
                        }
                        // Batch processing will be stopped via the _started flag on its next iteration
                    }

                    _started = false;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"ServiceBus listener exception during stopping ({_details.Value})");
                    throw;
                }
                finally
                {
                    _stopAsyncSemaphore.Release();
                    _logger.LogDebug($"ServiceBus listener stopped ({_details.Value})");
                }
            }
        }

        public void Cancel()
        {
            ThrowIfDisposed();
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

                _stopAsyncSemaphore.Dispose();
                _cancellationTokenSource.Dispose();

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
            using (CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cancellationTokenSource.Token))
            {
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
        }

        internal async Task ProcessSessionMessageAsync(IMessageSession session, Message message, CancellationToken cancellationToken)
        {
            using (CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cancellationTokenSource.Token))
            {
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
                            _logger.LogInformation($"Message processing has been stopped or cancelled ({_details.Value})");
                            return;
                        }

                        if (_isSessionsEnabled && ( receiver == null || receiver.IsClosedOrClosing))
                        {
                            try
                            {
                                receiver = await sessionClient.AcceptMessageSessionAsync();
                                receiver.PrefetchCount = _serviceBusOptions.PrefetchCount;
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
                                    List<Task> abandonTasks = new List<Task>();
                                    foreach (var lockTocken in messagesArray.Select(x => x.SystemProperties.LockToken))
                                    {
                                        abandonTasks.Add(receiver.AbandonAsync(lockTocken));
                                    }
                                    await Task.WhenAll(abandonTasks);
                                }
                            }
                        }
                        else
                        {
                            // Close the session and release the session lock after draining all messages for the accepted session.
                            if (_isSessionsEnabled)
                            {
                                await receiver.CloseAsync();
                            }
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                        // Ignore as we are stopping the host
                        _logger.LogInformation($"Message processing has been stopped or cancelled ({_details.Value})");
                    }
                    catch (Exception ex)
                    {
                        // Log another exception
                        _logger.LogError(ex, $"An unhandled exception occurred in the message batch receive loop ({_details.Value})");

                        if (_isSessionsEnabled && receiver != null)
                        {
                            // Attempt to close the session and release session lock to accept a new session on the next loop iteration
                            try
                            {
                                await receiver.CloseAsync();
                            }
                            catch
                            {
                                // Best effort
                                receiver = null;
                            }
                        }
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

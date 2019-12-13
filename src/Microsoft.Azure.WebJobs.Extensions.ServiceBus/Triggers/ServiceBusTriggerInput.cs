// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.


using System;
using System.Collections.Generic;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.ServiceBus.Listeners;

namespace Microsoft.Azure.WebJobs.ServiceBus
{
    // The core object we get when an ServiceBus is triggered. 
    // This gets converted to the user type (Message, string, poco, etc) 
    internal sealed class ServiceBusTriggerInput
    {
        private bool _isSingleDispatch;

        private ServiceBusTriggerInput() { }

        public IMessageReceiver MessageReceiver;

        public Message[] Messages { get; set; }

        public static ServiceBusTriggerInput CreateSingle(Message message)
        {
            return new ServiceBusTriggerInput
            {
                Messages = new Message[]
                {
                      message
                },
                _isSingleDispatch = true
            };
        }

        public static ServiceBusTriggerInput CreateBatch(Message[] messages)
        {
            return new ServiceBusTriggerInput
            {
                Messages = messages,
                _isSingleDispatch = false
            };
        }

        public bool IsSingleDispatch
        {
            get
            {
                return _isSingleDispatch;
            }
        }

        public TriggeredFunctionData GetTriggerFunctionData()
        {
            if (Messages.Length > 0)
            {
                Message message = Messages[0];
                if (IsSingleDispatch)
                {
                    Guid? parentId = ServiceBusCausalityHelper.GetOwner(message);
                    return new TriggeredFunctionData()
                    {
                        ParentId = parentId,
                        TriggerValue = this,
                        TriggerDetails = new Dictionary<string, string>()
                        {
                            { "MessageId", message.MessageId },
                            { "DeliveryCount", message.SystemProperties.DeliveryCount.ToString() },
                            { "EnqueuedTimeUtc", message.SystemProperties.EnqueuedTimeUtc.ToString() },
                            { "LockedUntilUtc", message.SystemProperties.LockedUntilUtc.ToString() },
                            { "SessionId", message.SessionId }
                        }
                    };
                }
                else
                {
                    Guid? parentId = ServiceBusCausalityHelper.GetOwner(message);

                    int length = Messages.Length;
                    string[] messageIds = new string[length];
                    int[] deliveryCounts = new int[length];
                    DateTime[] enqueuedTimes = new DateTime[length];
                    DateTime[] lockedUntils = new DateTime[length];
                    string sessionId = string.Empty;

                    sessionId = Messages[0].SystemProperties.LockedUntilUtc.ToString();
                    for (int i = 0; i < Messages.Length; i++)
                    {
                        messageIds[i] = Messages[i].MessageId;
                        deliveryCounts[i] = Messages[i].SystemProperties.DeliveryCount;
                        enqueuedTimes[i] = Messages[i].SystemProperties.EnqueuedTimeUtc;
                        lockedUntils[i] = Messages[i].SystemProperties.LockedUntilUtc;
                    }

                    return new TriggeredFunctionData()
                    {
                        ParentId = parentId,
                        TriggerValue = this,
                        TriggerDetails = new Dictionary<string, string>()
                        {
                            { "MessageIdArray", string.Join(",", messageIds)},
                            { "DeliveryCountArray", string.Join(",", deliveryCounts) },
                            { "EnqueuedTimeUtcArray", string.Join(",", enqueuedTimes) },
                            { "LockedUntilArray", string.Join(",", lockedUntils) },
                            { "SessionId", sessionId }
                        }
                    };
                }
            }
            return null;
        }
    }
}
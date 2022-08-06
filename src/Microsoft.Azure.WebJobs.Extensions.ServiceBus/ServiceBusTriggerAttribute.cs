﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Diagnostics;
using Microsoft.Azure.WebJobs.Description;
using Microsoft.Azure.ServiceBus;

namespace Microsoft.Azure.WebJobs
{
    /// <summary>
    /// Attribute used to bind a parameter to a ServiceBus Queue message, causing the function to run when a
    /// message is enqueued.
    /// </summary>
    /// <remarks>
    /// The method parameter type can be one of the following:
    /// <list type="bullet">
    /// <item><description>BrokeredMessage</description></item>
    /// <item><description><see cref="string"/></description></item>
    /// <item><description><see cref="T:byte[]"/></description></item>
    /// <item><description>A user-defined type (serialized as JSON)</description></item>
    /// </list>
    /// </remarks>
    [AttributeUsage(AttributeTargets.Parameter)]
    [DebuggerDisplay("{DebuggerDisplay,nq}")]
    [ConnectionProvider(typeof(ServiceBusAccountAttribute))]
    [Binding]
    public sealed class ServiceBusTriggerAttribute : Attribute, IConnectionProvider
    {
        private string _queueName;
        private string _topicName;
        private string _subscriptionName;
        private bool? _autoComplete = null;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceBusTriggerAttribute"/> class.
        /// </summary>
        /// <param name="queueName">The name of the queue to which to bind.</param>
        public ServiceBusTriggerAttribute(string queueName)
        {
            _queueName = queueName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceBusTriggerAttribute"/> class.
        /// </summary>
        /// <param name="topicName">The name of the topic to bind to.</param>
        /// <param name="subscriptionName">The name of the subscription in <paramref name="topicName"/> to bind to.</param>
        public ServiceBusTriggerAttribute(string topicName, string subscriptionName)
        {
            _topicName = topicName;
            _subscriptionName = subscriptionName;
        }

        // Empty Constructor required for generic operation.
        public ServiceBusTriggerAttribute()
        {
        }

        /// <summary>
        /// Gets or sets the app setting name that contains the Service Bus connection string.
        /// </summary>
        public string Connection { get; set; }

        /// <summary>
        /// Gets the name of the queue to which to bind.
        /// </summary>
        /// <remarks>When binding to a subscription in a topic, returns <see langword="null"/>.</remarks>
        public string QueueName
        {
            get { return _queueName; } set { _queueName = value; } 
        }

        /// <summary>
        /// Gets the name of the topic to which to bind.
        /// </summary>
        /// <remarks>When binding to a queue, returns <see langword="null"/>.</remarks>
        public string TopicName
        {
            get { return _topicName; }
        }

        /// <summary>
        /// Gets the name of the subscription in <see cref="TopicName"/> to bind to.
        /// </summary>
        /// <remarks>When binding to a queue, returns <see langword="null"/>.</remarks>
        public string SubscriptionName
        {
            get { return _subscriptionName; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the sessions are enabled.
        /// </summary>
        public bool IsSessionsEnabled { get; set; }

        /// <summary>
        /// Gets or sets the flag whether trigger should automatically call complete after processing, or if the function code will manually call complete.
        /// </summary>
        public bool AutoComplete
        {
            get
            {
                return _autoComplete.HasValue ? _autoComplete.Value : true;
            }
            set
            {
                _autoComplete = value;
            }
        }

        /// <summary>
        /// Gets a boolean to check if auto complete option was set on the trigger.
        /// </summary>
        internal bool IsAutoCompleteOptionSet { get { return _autoComplete.HasValue; } }

        private string DebuggerDisplay
        {
            get
            {
                if (_queueName != null)
                {
                    return _queueName;
                }
                else
                {
                    return _topicName + "/" + _subscriptionName;
                }
            }
        }
    }
}

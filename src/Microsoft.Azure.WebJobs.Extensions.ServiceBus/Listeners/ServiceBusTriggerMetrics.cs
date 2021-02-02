// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Azure.WebJobs.Host.Scale;
using System;

namespace Microsoft.Azure.WebJobs.ServiceBus.Listeners
{
    internal class ServiceBusTriggerMetrics : ScaleMetrics
    {
        /// <summary>
        /// The number of messages currently in the queue/topic.
        /// </summary>
        public long MessageCount { get; set; }

        /// <summary>
        /// The number of partitions.
        /// </summary>
        public int PartitionCount { get; set; }

        /// <summary>
        /// The length of time the next message has been
        /// sitting there.
        /// </summary>
        public TimeSpan QueueTime { get; set; }

        /// <summary>
        /// The delivery count of the next message
        /// </summary>
        public int DeliveryCount { get; set; } = -1;
    }
}

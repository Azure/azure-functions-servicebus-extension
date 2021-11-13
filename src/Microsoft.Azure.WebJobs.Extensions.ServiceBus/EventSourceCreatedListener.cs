// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Text;

namespace Microsoft.Azure.WebJobs.ServiceBus
{
    sealed class EventSourceCreatedListener : EventListener
    {
        private ILogger _logger;

        public EventSourceCreatedListener(ILoggerFactory loggerFactory) : base()
        {
            _logger = loggerFactory.CreateLogger<EventSourceCreatedListener>();
        }

        protected override void OnEventSourceCreated(EventSource eventSource)
        {
            base.OnEventSourceCreated(eventSource);

            if (eventSource.Name == "Microsoft-Azure-ServiceBus")
            {
                EnableEvents(eventSource, EventLevel.LogAlways, EventKeywords.All);
            }
        }

        protected override void OnEventWritten(EventWrittenEventArgs eventData)
        {
            StringBuilder messageBuilder = new StringBuilder($"EventId=\"{eventData.EventId}\", EventName=\"{eventData.EventName}\", Message=\"{eventData.Message}\"");
            for (int i = 0; i < eventData.Payload.Count; i++)
            {
                string payloadString = eventData.Payload[i]?.ToString() ?? string.Empty;
                messageBuilder.Append($", Name{i}=\"{eventData.PayloadNames[i]}\", Value{i}=\"{payloadString}\"");
            }

            _logger.LogDebug(messageBuilder.ToString());
        }
    }
}
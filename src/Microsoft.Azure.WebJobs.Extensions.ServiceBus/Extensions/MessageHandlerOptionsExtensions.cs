// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

namespace Microsoft.Azure.WebJobs.ServiceBus.Extensions
{
    internal static class MessageHandlerOptionsExtensions
    {
        public static bool ShouldAutoRenewLock(this MessageHandlerOptions options)
        {
            return options.MaxAutoRenewDuration > TimeSpan.Zero;
        }

        public static async Task RaiseExceptionReceived(this MessageHandlerOptions options, ExceptionReceivedEventArgs eventArgs)
        {
            try
            {
                await options.ExceptionReceivedHandler(eventArgs).ConfigureAwait(false);
            }
            catch
            {
                // TODO: log
                //MessagingEventSource.Log.ExceptionReceivedHandlerThrewException(exception);
            }
        }
    }
}

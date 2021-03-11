// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

namespace Microsoft.Azure.WebJobs.ServiceBus
{
    internal class Utility
    {
        /// <summary>
        /// Returns processor count for a worker, for consumption plan always returns 1
        /// </summary>
        /// <returns></returns>
        public static int GetProcessorCount()
        {
            string skuValue = Environment.GetEnvironmentVariable(Constants.AzureWebsiteSku);
            return string.Equals(skuValue, Constants.DynamicSku, StringComparison.OrdinalIgnoreCase) ? 1 : Environment.ProcessorCount;
        }

        public static int GetEffectiveCoresCount()
        {
            // When not running on VMSS, the dynamic plan has some limits that mean that a given instance is using effectively a single core,
            // so we should not use Environment.Processor count in this case.
            var effectiveCores = (IsWindowsConsumption() && !IsVMSS()) ? 1 : Environment.ProcessorCount;
            return effectiveCores;
        }

        public static bool IsWindowsConsumption()
        {
            string value = Environment.GetEnvironmentVariable(Constants.AzureWebsiteSku);
            return string.Equals(value, Constants.DynamicSku, StringComparison.OrdinalIgnoreCase);
        }

        public static bool IsVMSS()
        {
            string value = Environment.GetEnvironmentVariable("RoleInstanceId");
            return value != null && value.IndexOf("HostRole", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        public static void ScheduleTask(Func<Task> func)
        {
            Task.Run(async () =>
            {
                try
                {
                    await func().ConfigureAwait(false);
                }
                catch
                {
                    // TODO: log this
                    //MessagingEventSource.Log.ScheduleTaskFailed(func, ex);
                }
            });
        }

        public static TimeSpan CalculateRenewAfterDuration(DateTime lockedUntilUtc)
        {
            var remainingTime = lockedUntilUtc - DateTime.UtcNow;

            if (remainingTime < TimeSpan.FromMilliseconds(400))
            {
                return TimeSpan.Zero;
            }

            var buffer = TimeSpan.FromTicks(Math.Min(remainingTime.Ticks / 2, Constants.MaximumRenewBufferDuration.Ticks));
            var renewAfter = remainingTime - buffer;

            return renewAfter;
        }

        public static bool ShouldRetry(Exception exception)
        {
            var serviceBusException = exception as ServiceBusException;
            return serviceBusException?.IsTransient == true;
        }
    }
}

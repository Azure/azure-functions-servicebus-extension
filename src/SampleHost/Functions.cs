using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace SampleHost
{
    public class Functions
    {
        //[Disable]
        public static async Task ProcessMessage([ServiceBusTrigger("concurrency-work-items-1", Connection = "AzureWebJobsServiceBus")] string myQueueItem, ILogger log)
        {
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {myQueueItem}");

            await GenerateLoadAllCoresAsync();
        }

        [Disable]
        public static async Task ConcurrencyTest_Lightweight([QueueTrigger("concurrency-work-items-2")] string message, ILogger log)
        {
            log.LogInformation($"C# Queue trigger function processed: {message}");

            await Task.Delay(250);
        }

        public static async Task GenerateLoadAllCoresAsync()
        {
            int cores = GetEffectiveCoresCount();
            List<Task> tasks = new List<Task>();
            for (int i = 0; i < cores; i++)
            {
                var task = Task.Run(() => GenerateLoad());
                tasks.Add(task);
            }

            await Task.WhenAll(tasks);
        }

        public static void GenerateLoad()
        {
            int start = 2000;
            int numPrimes = 200;

            for (int i = start; i < start + numPrimes; i++)
            {
                FindPrimeNumber(i);
            }
        }

        public static long FindPrimeNumber(int n)
        {
            int count = 0;
            long a = 2;
            while (count < n)
            {
                long b = 2;
                int prime = 1; // to check if found a prime
                while (b * b <= a)
                {
                    if (a % b == 0)
                    {
                        prime = 0;
                        break;
                    }
                    b++;
                }
                if (prime > 0)
                {
                    count++;
                }
                a++;
            }
            return (--a);
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
            string value = Environment.GetEnvironmentVariable("WEBSITE_SKU");
            return string.Equals(value, "Dynamic", StringComparison.OrdinalIgnoreCase);
        }

        public static bool IsVMSS()
        {
            string value = Environment.GetEnvironmentVariable("RoleInstanceId");
            return value != null && value.IndexOf("HostRole", StringComparison.OrdinalIgnoreCase) >= 0;
        }
    }
}

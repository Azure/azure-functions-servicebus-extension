// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

namespace Microsoft.Azure.WebJobs.ServiceBus
{
    public class ServiceBusConnection
    {
        public string ConnectionValue { get; }
        public bool IsManagedIdentityConnection { get; }

        public ServiceBusConnection(string connectionValue, bool isManagedIdentityConnection)
        {
            ConnectionValue = connectionValue;
            IsManagedIdentityConnection = isManagedIdentityConnection;
        }
    }
}
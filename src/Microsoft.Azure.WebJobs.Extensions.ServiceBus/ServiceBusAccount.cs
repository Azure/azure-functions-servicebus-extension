// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Globalization;
using Microsoft.Azure.WebJobs.Logging;
using Microsoft.Extensions.Configuration;

namespace Microsoft.Azure.WebJobs.ServiceBus
{
    internal class ServiceBusAccount
    {
        private readonly ServiceBusOptions _options;
        private readonly IConnectionProvider _connectionProvider;
        private readonly IConfiguration _configuration;
        private ServiceBusConnection _connection;

        public ServiceBusAccount(ServiceBusOptions options, IConfiguration configuration, IConnectionProvider connectionProvider = null)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _configuration = configuration;
            _connectionProvider = connectionProvider;
        }

        internal ServiceBusAccount()
        {
        }
        
        public virtual ServiceBusConnection Connection
        {
            get
            {
                if (_connection == null)
                {
                    _connection = GetConnectionValue();
                }

                return _connection;
            }
        }

        private ServiceBusConnection GetConnectionValue()
        {
            var isManagedIdentityConnection = _options.UseManagedServiceIdentity;
            var connection = _options.UseManagedServiceIdentity
                ? _options.Endpoint
                : _options.ConnectionString;

            if (_connectionProvider != null && !string.IsNullOrEmpty(_connectionProvider.Connection))
            {
                connection = _configuration.GetWebJobsConnectionString(_connectionProvider.Connection);
            }

            if (string.IsNullOrEmpty(connection))
            {
                var message = isManagedIdentityConnection 
                    ? "Microsoft Azure WebJobs SDK ServiceBus endpoint '{0}' is missing or empty."
                    : "Microsoft Azure WebJobs SDK ServiceBus connection string '{0}' is missing or empty.";
                throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, message,
                        Sanitizer.Sanitize(_connectionProvider?.Connection) ?? Constants.DefaultConectionSettingStringName));
            }

            var isEndpoint = Uri.TryCreate(connection, UriKind.Absolute, out _);
            if (!isEndpoint)
            {
                //assuming that the connection is connection string
                isManagedIdentityConnection = false;
            }

            return new ServiceBusConnection(connection, isManagedIdentityConnection);
        }
    }
}

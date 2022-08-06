// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs.ServiceBus.Listeners
{
    public class ScaleMonitorContext
    {
        // plan to obsolute
        private IDictionary<string, string> _config = new Dictionary<string, string>();

        // plan to obsolute
        public ILogger Logger { get; set; }

        public ILoggerFactory LoggerFactory { get; set; }

        public string TriggerData { get; set; }

        public string ExtensionOptions { get; set; } // payload of admin/host/config API
        
        public IDictionary<string, string> AppSettings { get; set; }

        public string FunctionId { get; set; }

        public string FunctionName // This one is created by TriggerData
        {
            get
            {
                var triggerData = JObject.Parse(TriggerData);
                return triggerData["functionName"]?.ToString() ?? "";
            }
            set
            {
                // Keep this for Deserialize probably not needed.
            }
        }

        public List<ManagedIdentityInformation> ManagedIdentities { get; set; }

        public IConfiguration Configration { get; set; } // Convert from AppSettings

        public INameResolver NameResolver
        {
            get
            {
                return new DefaultNameResolver(Configration); // consider caching or immutable
            } 
        }

        public string this[string key]
        {
            get { return _config[key]; }
        }

        public T GetTriggerAttribute<T>()
        {
            // Write a logic hydrate T from TriggerData
            T trigggerAttribute = JsonConvert.DeserializeObject<T>(TriggerData);

            return trigggerAttribute;
        }

        // Option will be resolve by binding from IConfiguration.
        public T GetExtensionOption<T>(string extensionName)
        {
            // string DefaultSection = "AzureFunctionsJobHost:extensions:";
            var section = Configration.GetWebJobsExtensionConfigurationSection(extensionName);
            var instance = Activator.CreateInstance<T>();
            section.Bind(instance);
            return instance;
        }
    }
}

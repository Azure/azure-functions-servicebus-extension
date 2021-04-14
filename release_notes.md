### Release notes
<!-- Please add your release notes in the following format:
- My change description (#PR)
-->
#### Version 4.2.2
- Modified behavior of ServiceBusListener.Cancel() to not set the cancellation token as this was inconsistent with other extensions and impacting WebJobs on shutdown by resulting in auto-abandoned messages in the interval between when ServiceBusListener.Cancel() is called and ServiceBusListener.StopAsync() completes.

**Release sprint:** Sprint 98
[ [bugs](https://github.com/Azure/azure-functions-servicebus-extension/issues?q=is%3Aissue+milestone%3A%22Functions+Sprint+98%22+label%3Abug+is%3Aclosed) | [features](https://github.com/Azure/azure-functions-servicebus-extension/issues?q=is%3Aissue+milestone%3A%22Functions+Sprint+98%22+label%3Afeature+is%3Aclosed) ]

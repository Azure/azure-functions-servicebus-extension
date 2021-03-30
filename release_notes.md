### Release notes
<!-- Please add your release notes in the following format:
- My change description (#PR)
-->
#### Version 4.2.2
- Modified behavior of ServiceBusListener.Cancel() to call StopAsync() instead of cancelling the cancellation token as this was inconsistent with other extensions and impacting WebJobs on shutdown.
- When the cancellation token passed to functions is cancelled, messages are no longer automatically abandoned by the default MessageProcessor implementation.  This behavior was changed to allow in-flight function executions to complete successfully via "drain mode" which will provide functions with an extended period of time when an instance is shutting down as a result of scaling in.

**Release sprint:** Sprint 98
[ [bugs](https://github.com/Azure/azure-functions-servicebus-extension/issues?q=is%3Aissue+milestone%3A%22Functions+Sprint+98%22+label%3Abug+is%3Aclosed) | [features](https://github.com/Azure/azure-functions-servicebus-extension/issues?q=is%3Aissue+milestone%3A%22Functions+Sprint+98%22+label%3Afeature+is%3Aclosed) ]

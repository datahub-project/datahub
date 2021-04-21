DataHub React Analytics

We leverage a thin wrapper over the Analytics API to emit tracking
analytics to a backend provider.

We've implemented official support for - Google Analytics (Universal Analaytics) - Mixpanel

Currently, enabling tracking requires code changes inside of the datahub-web-react package. Specifically, changes to the "plugin" file of your choice are required to

    a. Enable events to be sent to the analytics provider
    b. Configure the provider credentials

Soon, we intend to make this available via environment variables such that
no code changes will be required to configure.

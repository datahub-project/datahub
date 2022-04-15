const config: any = {
    // Uncomment below to configure analytics.
    // googleAnalytics: {
    //    trackingId: 'UA-24123123-01',
    // },
    mixpanel:
        (process.env.REACT_APP_MIXPANEL_EVENTS_ENABLED || 'false').toLowerCase() === 'true'
            ? {
                  token: process.env.REACT_APP_MIXPANEL_TOKEN,
              }
            : null,
    // amplitude: {
    //    apiKey: 'c5c212632315d19c752ab083bc7c92ff',
    // },
    // logging: true,
    datahub: {
        enabled: true,
    },
};

export default config;

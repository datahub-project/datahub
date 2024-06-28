# DataHub React Analytics

## About

The DataHub React application can be configured to emit a set of standardized product analytics events to multiple backend providers including

-   Mixpanel
-   Amplitude
-   Google Analytics

This provides operators of DataHub with visibility into how their users are engaging with the platform, allowing them to answer questions around weekly active users, the most used features, the least used features, and more.

To accomplish this, we have built a small extension on top of the popular [Analytics](https://www.npmjs.com/package/analytics) npm package. This package was chosen because it offers a clear pathway to extending support to many other providers, all of which you can find listed [here](https://github.com/DavidWells/analytics#analytic-plugins).

## Configuring an Analytics Provider

Currently, configuring an analytics provider requires that you fork DataHub & modify code. As described in 'Coming Soon', we intend to improve this process by implementing no-code configuration.

### Mixpanel

1. Open `datahub-web-react/src/conf/analytics.ts`
2. Uncomment the `mixpanel` field within the `config` object.
3. Replace the sample `token` with the API token provided by Mixpanel.
4. Rebuild & redeploy `datahub-frontend-react` to start tracking.

```typescript
const config: any = {
    mixpanel: {
        token: 'fad1285da4e618b618973cacf6565e61',
    },
};
```

### Amplitude

1. Open `datahub-web-react/src/conf/analytics.ts`
2. Uncomment the `amplitude` field within the `config` object.
3. Replace the sample `apiKey` with the key provided by Amplitude.
4. Rebuild & redeploy `datahub-frontend-react` to start tracking.

```typescript
const config: any = {
    amplitude: {
        apiKey: 'c5c212632315d19c752ab083bc7c92ff',
    },
};
```

### Google Analytics

1. Open `datahub-web-react/src/conf/analytics.ts`
2. Uncomment the `googleAnalytics` field within the `config`.
3. Replace the sample `measurementIds` with the one provided by Google Analytics.
4. Rebuild & redeploy `datahub-frontend-react` to start tracking.

Example:

```typescript
const config: any = {
    googleAnalytics: {
        measurementIds: ['G-ATV123'],
    },
};
```

## Verifying your Analytics Setup

To verify that analytics are being sent to your provider, you can inspect the networking tab of a Google Chrome inspector window:

With DataHub open on Google Chrome

1. Right click, then Inspect
2. Click 'Network'
3. Issue a search in DataHub
4. Inspect the outbound traffic for requests routed to your analytics provider.

## Development

### Adding a plugin

To add a new plugin from the [Analytics](https://www.npmjs.com/package/analytics) library:

1. Add a new file under `src/app/analytics/plugin` named based on the plugin
2. Extract configs from the analytics config object required to instantiate the plugin
3. Instantiate the plugin
4. Export a default object with 'isEnabled' and 'plugin' fields
5. Import / Export the new plugin module from `src/app/analytics/plugin/index.js`

If you're unsure, check out the existing plugin implements as examples. Before contributing a plugin, please be sure to verify the integration by viewing the product metrics in the new analytics provider.

### Adding an event

To add a new DataHub analytics event, make the following changes to `src/app/analytics/event.ts`:

1. Add a new value to the `EventType` enum

```typescript
   export enum EventType {
    LogInEvent,
    LogOutEvent,
    ...,
    MyNewEvent
}
```

2. Create a new interface extending `BaseEvent`

```typescript
export interface MyNewEvent extends BaseEvent {
    type: EventType.MyNewEvent; // must be the type you just added
    ... your event's custom fields
}
```

3. Add the interface to the exported `Event` type.

```typescript
export type Event =
    | LogInEvent
    | LogOutEvent
    ....
    | MyNewEvent
```

### Emitting an event

Emitting a tracking DataHub analytics event is a 2-step process:

1. Import relevant items from `analytics` module

```typescript
import analytics, { EventType } from '../analytics';
```

2. Call the `event` method, passing in an event object of the appropriate type

```typescript
analytics.event({ type: EventType.MyNewEvent, ...my event fields });
```

### Debugging: Enabling Event Logging

To log events to the console for debugging / verification purposes

1. Open `datahub-web-react/src/conf/analytics.ts`
2. Uncomment `logging: true` within the `config` object.
3. Rebuild & redeploy `datahub-frontend-react` to start logging all events to your browser's console.

## Coming Soon

In the near future, we intend to

1. Send product analytics events back to DataHub itself, using them as feedback to improve the product experience.
2. No-code configuration of Analytics plugins. This will be achieved using server driven configuration for the React app.

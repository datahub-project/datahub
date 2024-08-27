import analyticsConfig from '../../../conf/analytics';

const { datahub } = analyticsConfig;
const isEnabled: boolean = (datahub && datahub.enabled) || false;

const track = (payload) => {
    fetch('/track', {
        method: 'POST',
        cache: 'no-cache',
        credentials: 'same-origin',
        headers: {
            'Content-Type': 'application/json',
        },
        referrerPolicy: 'no-referrer',
        body: JSON.stringify(payload),
    });
};

const datahubPlugin = () => {
    return {
        /* All plugins require a name */
        name: 'datahub',
        initialize: () => {},
        identify: () => {},
        loaded: () => {
            return true;
        },
        page: ({ payload }) => {
            track(payload.properties);
        },
        track: ({ payload }) => {
            track(payload.properties);
        },
    };
};

export default {
    isEnabled,
    plugin: isEnabled && datahubPlugin(),
};

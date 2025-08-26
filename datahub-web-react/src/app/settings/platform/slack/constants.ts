export const DEFAULT_SETTINGS = {
    defaultChannelName: undefined,
    botToken: undefined,
};

export const DEFAULT_CONNECTION = {
    botToken: undefined,
    appConfigToken: undefined,
    appConfigRefreshToken: undefined,
};

export const SLACK_CONNECTION_ID = '__system_slack-0';
export const SLACK_CONNECTION_URN = `urn:li:dataHubConnection:${SLACK_CONNECTION_ID}`;
export const SLACK_PLATFORM_URN = 'urn:li:dataPlatform:slack';

export const BOT_TOKEN_SELECT_ID = 'bot-token';
export const APP_CONFIG_SELECT_ID = 'app-config-token';
export const SLACK_CONNECTION_OPTIONS = [
    { label: 'App Configuration Token', value: APP_CONFIG_SELECT_ID },
    { label: 'Bot Token', value: BOT_TOKEN_SELECT_ID },
];

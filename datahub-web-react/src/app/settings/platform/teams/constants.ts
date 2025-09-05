export const DEFAULT_SETTINGS = {
    defaultChannelName: undefined,
    appId: undefined,
    tenantId: undefined,
};

export const DEFAULT_CONNECTION = {
    appId: undefined,
    appSecret: undefined,
    tenantId: undefined,
};

export const TEAMS_CONNECTION_ID = '__system_teams-0';
export const TEAMS_CONNECTION_URN = `urn:li:dataHubConnection:${TEAMS_CONNECTION_ID}`;
export const TEAMS_PLATFORM_URN = 'urn:li:dataPlatform:teams';

export const APP_ID_SELECT_ID = 'app-id';
export const APP_SECRET_SELECT_ID = 'app-secret';
export const TENANT_ID_SELECT_ID = 'tenant-id';
export const TEAMS_CONNECTION_OPTIONS = [
    { label: 'App ID', value: APP_ID_SELECT_ID },
    { label: 'App Secret', value: APP_SECRET_SELECT_ID },
    { label: 'Tenant ID', value: TENANT_ID_SELECT_ID },
];

export const TEAMS_CONNECTION_ID = '__system_teams-0';
export const TEAMS_CONNECTION_URN = `urn:li:dataHubConnection:${TEAMS_CONNECTION_ID}`;
export const TEAMS_PLATFORM_URN = 'urn:li:dataPlatform:microsoft-teams';

export const getTeamsConnection = (url: string): string => {
    const jsonObject = {
        connection: {
            notifications_webhook_url: url,
        },
    };
    return JSON.stringify(jsonObject);
};

export const getWebhookURL = (json) => {
    const parsedJson = JSON.parse(json);
    return parsedJson.connection?.notifications_webhook_url;
};

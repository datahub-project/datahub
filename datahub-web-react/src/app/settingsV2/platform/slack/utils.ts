import { SlackConnection } from './types';

export const SLACK_INSTALL_REDIRECT_PATH = '/integrations/slack/install';

/**
 * Decodes the Slack Configuration JSON to a well-formed object.
 */
export const decodeSlackConnection = (rawJson: string): SlackConnection | null => {
    const parsedJson = JSON.parse(rawJson);
    return {
        botToken: parsedJson.bot_token,
        appConfigToken: parsedJson.app_config_tokens?.access_token,
        appConfigRefreshToken: parsedJson.app_config_tokens?.refresh_token,
        json: parsedJson,
    };
};

/**
 * Encodes the Slack Configuration object to JSON.
 */
export const encodeSlackConnection = (config: SlackConnection): string => {
    const jsonObject = {
        ...config.json,
        app_config_tokens: {
            access_token: config.appConfigToken,
            refresh_token: config.appConfigRefreshToken,
        },
        bot_token: config.botToken,
    };
    return JSON.stringify(jsonObject);
};

/**
 * Performs a hard browser redirect to the Slack install link.
 */
export const redirectToSlackInstall = () => {
    window.location.replace(SLACK_INSTALL_REDIRECT_PATH);
};

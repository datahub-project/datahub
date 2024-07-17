import { SlackConnection } from './types';

export const SLACK_INSTALL_REDIRECT_PATH = '/integrations/slack/install';

/**
 * Decodes the Slack Configuration JSON to a well-formed object.
 */
export const decodeSlackConnection = (rawJson: string): SlackConnection | null => {
    const parsedJson = JSON.parse(rawJson);
    return {
        botToken: parsedJson.bot_token,
        signingSecret: parsedJson.app_details?.signing_secret,
        appConfigToken: parsedJson.app_config_tokens?.access_token,
        appConfigRefreshToken: parsedJson.app_config_tokens?.refresh_token,
        json: parsedJson,
    };
};

/**
 * Encodes the Slack Configuration object to JSON.
 */
export const encodeSlackConnection = (config: SlackConnection): string => {
    const appDetails =
        typeof config.json === 'object' && 'app_details' in config.json ? config.json.app_details : undefined;
    const jsonObject = {
        ...config.json,
        app_config_tokens: {
            access_token: config.appConfigToken,
            refresh_token: config.appConfigRefreshToken,
        },
        app_details: config.signingSecret
            ? {
                  ...appDetails,
                  signing_secret: config.signingSecret,
              }
            : appDetails,
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

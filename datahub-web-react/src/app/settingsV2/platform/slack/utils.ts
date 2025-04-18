import * as QueryString from 'query-string';
import { useLocation } from 'react-router';

import { SlackConnection } from '@app/settingsV2/platform/slack/types';

export const SLACK_INSTALL_REDIRECT_PATH = '/integrations/slack/install';
export const SLACK_REFRESH_INSTALLATION_REDIRECT_PATH = '/integrations/slack/refresh-installation';

export function useShouldDisplayBotTokensTabFromQueryParams() {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    return params.display_all_configs === 'true';
}

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
        appId: parsedJson.app_details?.app_id,
        json: parsedJson,
    };
};

/**
 * Encodes the Slack Configuration object to JSON.
 */
export const encodeSlackConnection = (config: SlackConnection, isUsingAppConfigTokens: boolean): string => {
    let appDetails =
        typeof config.json === 'object' && 'app_details' in config.json ? config.json.app_details : undefined;
    if (config.appId) {
        if (!appDetails) {
            appDetails = {};
        }
        appDetails.app_id = config.appId;
    }
    if (config.signingSecret) {
        if (!appDetails) {
            appDetails = {};
        }
        appDetails.signing_secret = !isUsingAppConfigTokens ? config.signingSecret : undefined;
    }

    const jsonObject = {
        ...config.json,
        app_config_tokens: {
            access_token: config.appConfigToken,
            refresh_token: config.appConfigRefreshToken,
        },
        app_details: appDetails,
        bot_token: !isUsingAppConfigTokens ? config.botToken : undefined,
    };
    return JSON.stringify(jsonObject);
};

/**
 * Performs a hard browser redirect to the Slack install link.
 */
export const redirectToSlackInstall = (queryParams?: { requestMinimalSlackPermissions: string }) => {
    const params = queryParams && new URLSearchParams(queryParams);
    window.location.replace(`${SLACK_INSTALL_REDIRECT_PATH}${params ? `?${params.toString()}` : ''}`);
};

/**
 * Performs a hard browser redirect to the Slack refresh-installation link.
 */
export const redirectToSlackRefreshInstallation = (queryParams?: { requestMinimalSlackPermissions: string }) => {
    const params = queryParams && new URLSearchParams(queryParams);
    window.location.replace(`${SLACK_REFRESH_INSTALLATION_REDIRECT_PATH}${params ? `?${params.toString()}` : ''}`);
};

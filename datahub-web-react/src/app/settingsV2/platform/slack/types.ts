/**
 * Slack integration specific configs
 */
export type SlackConnection = {
    /**
     * The bot token for Slack
     */
    botToken?: string | null;
    /**
     * The signing secret for Slack
     */
    signingSecret?: string | null;
    /**
     * The app id for Slack
     */
    appId?: string | null;
    /**
     * The app configuration token
     */
    appConfigToken?: string | null;
    /**
     * The app config refresh token
     */
    appConfigRefreshToken?: string | null;
    /**
     * The raw JSON of the connection
     */
    json?: any;
};

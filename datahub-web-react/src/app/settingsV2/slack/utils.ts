// Import connection query for Slack-specific connection retrieval
import { useConnectionQuery } from '@graphql/connection.generated';

/**
 * Centralized Slack integration utilities.
 *
 * This module provides the single source of truth for checking Slack platform configuration status.
 */

// Slack connection constants
export const SLACK_CONNECTION_ID = '__system_slack-0';
export const SLACK_CONNECTION_URN = `urn:li:dataHubConnection:${SLACK_CONNECTION_ID}`;

/**
 * Decode Slack connection data from DataHubConnection JSON blob.
 */
export const decodeSlackConnection = (jsonString: string) => {
    try {
        const parsed = JSON.parse(jsonString);
        return {
            bot_token: parsed.bot_token || '',
            team_id: parsed.team_id || '',
            app_details: parsed.app_details || null,
            // Add other fields as needed
        };
    } catch {
        return { bot_token: '', team_id: '', app_details: null };
    }
};

/**
 * Check if Slack platform integration is configured.
 *
 * This is the SINGLE function that all parts of the system should use
 * to determine if Slack integration is enabled at the platform level.
 *
 * Slack integration is considered configured when:
 * - A DataHubConnection exists with the Slack connection URN
 * - The connection contains a valid bot_token or app_details
 *
 * This is distinct from:
 * - Platform notification settings (defaultChannelName in GlobalSettings)
 * - Personal notification settings (userHandle in user settings)
 *
 * @param connectionData - Data from useConnectionQuery for SLACK_CONNECTION_URN
 * @returns true if Slack platform integration is configured
 */
export const isSlackIntegrationConfigured = (connectionData: any): boolean => {
    const existingConnJson = connectionData?.connection?.details?.json;
    if (!existingConnJson) {
        return false;
    }

    try {
        const slackConnection = decodeSlackConnection(existingConnJson.blob as string);
        return !!(slackConnection.bot_token || slackConnection.app_details);
    } catch {
        return false;
    }
};

/**
 * Check if Slack platform integration is configured using GlobalSettings data.
 *
 * This function checks the configuration and indicates
 * that Slack OAuth connection exists.
 *
 * @param globalSettings - Data from useGetGlobalSettingsQuery
 * @returns true if Slack platform integration is configured
 */
export const isSlackIntegrationConfiguredFromSettings = (globalSettings: any): boolean => {
    // Check configuration
    const hasConfig = !!globalSettings?.globalSettings?.integrationSettings?.slackSettings?.defaultChannelName;

    return hasConfig;
};

/**
 * Slack integration platform identifier
 */
export const SLACK_INTEGRATION_PLATFORM = 'slack';

/**
 * Slack-specific connection retriever function.
 * Extracts Slack connection data from connection query result.
 *
 * @param connectionData - Data from useConnectionQuery for SLACK_CONNECTION_URN
 * @returns Slack connection data or null if not found/configured
 */
export const getSlackConnection = (connectionData: any) => {
    if (!connectionData?.connection) {
        return null;
    }

    try {
        const jsonData = connectionData.connection.details?.json;
        if (jsonData?.blob) {
            const slackData = decodeSlackConnection(jsonData.blob);
            // Only return connection if it has a valid bot_token or app_details
            return slackData.bot_token || slackData.app_details ? slackData : null;
        }
    } catch {
        return null;
    }

    return null;
};

/**
 * Hook to check if Slack platform integration is configured.
 *
 * Simply checks if a Slack connection entity exists with a name.
 * If the connection exists, we assume it's properly configured.
 * This avoids parsing the JSON blob for better performance.
 */
export const useIsSlackIntegrationConfigured = () => {
    const {
        data: connectionData,
        loading,
        error,
    } = useConnectionQuery({
        variables: { urn: SLACK_CONNECTION_URN },
        errorPolicy: 'ignore', // Don't fail if connection query has issues
        fetchPolicy: 'cache-first', // Use cache to avoid repeated expensive queries
    });

    // If query is loading, return false (don't assume configured)
    if (loading) {
        return false;
    }

    // If query failed, return false
    if (error) {
        return false;
    }

    // If the connection exists with a name, Slack is configured
    return !!connectionData?.connection?.details?.name;
};

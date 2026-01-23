import { PLATFORM_URN_TO_LOGO } from '@app/ingestV2/source/builder/constants';
import dbtLogo from '@src/images/dbtlogo.png';
import githubLogo from '@src/images/githublogo.png';
import notionLogo from '@src/images/notionlogo.png';
import slackLogo from '@src/images/slacklogo.png';
import snowflakeLogo from '@src/images/snowflakelogo.png';

/**
 * Known MCP server domains mapped to local logos.
 * Only add entries here for MCP servers where we have local logo assets.
 * If no match is found, returns null (component should show fallback icon).
 */
export const KNOWN_MCP_LOGOS: Record<string, string> = {
    // Data platforms
    'cloud.getdbt.com': dbtLogo,
    'getdbt.com': dbtLogo,
    'dbt.com': dbtLogo,
    'snowflake.com': snowflakeLogo,
    'snowflakecomputing.com': snowflakeLogo,
    // Common MCP integrations
    'github.com': githubLogo,
    'slack.com': slackLogo,
    'notion.so': notionLogo,
    'notion.com': notionLogo,
};

/**
 * Extracts hostname from a URL string, handling URLs with or without protocol.
 */
export function extractHostname(url: string): string | null {
    try {
        // If URL has a protocol, parse it normally
        if (url.includes('://')) {
            return new URL(url).hostname.toLowerCase();
        }
        // Otherwise, add https:// and try again
        return new URL(`https://${url}`).hostname.toLowerCase();
    } catch {
        return null;
    }
}

/**
 * Tries to match a display name to a known data platform logo.
 * Uses PLATFORM_URN_TO_LOGO from ingestion constants.
 */
export function getLogoFromDisplayName(displayName: string): string | null {
    if (!displayName) return null;

    const lowerName = displayName.toLowerCase();
    const platformKeys = Object.keys(PLATFORM_URN_TO_LOGO);

    const matchingKey = platformKeys.find((key) => {
        const platformName = key.replace('urn:li:dataPlatform:', '');
        return lowerName.includes(platformName);
    });

    if (matchingKey) {
        return PLATFORM_URN_TO_LOGO[matchingKey];
    }

    return null;
}

/**
 * Gets the logo URL for an MCP server.
 * Priority:
 * 1. Match display name to PLATFORM_URN_TO_LOGO
 * 2. Match URL domain to KNOWN_MCP_LOGOS
 * 3. Return null (component should show fallback icon)
 *
 * Note: Does NOT use external services like Google Favicon.
 */
export function getPluginLogoUrl(
    displayName: string | undefined | null,
    url: string | undefined | null,
): string | null {
    // First, try to match display name to known platforms
    if (displayName) {
        const platformLogo = getLogoFromDisplayName(displayName);
        if (platformLogo) return platformLogo;
    }

    // Then, try to match URL to known domains
    if (url) {
        const hostname = extractHostname(url);
        if (hostname) {
            const knownDomain = Object.keys(KNOWN_MCP_LOGOS).find((domain) => hostname.includes(domain));
            if (knownDomain) {
                return KNOWN_MCP_LOGOS[knownDomain];
            }
        }
    }

    // No match found - return null, component will show fallback icon
    return null;
}

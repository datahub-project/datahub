import { PLATFORM_URN_TO_LOGO } from '@app/ingestV2/source/builder/constants';
import dbtLogo from '@src/images/dbtlogo.png';
import githubLogo from '@src/images/githublogo.png';
import gleanLogo from '@src/images/gleanlogo.png';
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
    'glean.com': gleanLogo,
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
 * Tries to match a display name to a known MCP logo by keyword.
 * E.g., "GitHub MCP Server" -> matches "github" -> githubLogo
 */
export function getLogoFromDisplayNameMcp(displayName: string): string | null {
    if (!displayName) return null;

    const lowerName = displayName.toLowerCase();

    // Map of keywords to their domain keys in KNOWN_MCP_LOGOS
    const keywordToDomain: Record<string, string> = {
        github: 'github.com',
        slack: 'slack.com',
        notion: 'notion.so',
        dbt: 'dbt.com',
        snowflake: 'snowflake.com',
        glean: 'glean.com',
    };

    const matchingKeyword = Object.keys(keywordToDomain).find((keyword) => lowerName.includes(keyword));
    if (matchingKeyword) {
        const domain = keywordToDomain[matchingKeyword];
        return KNOWN_MCP_LOGOS[domain] || null;
    }

    return null;
}

/**
 * Known platform names mapped from URL domains.
 * Used for analytics tracking events.
 */
const DOMAIN_TO_PLATFORM: Record<string, string> = {
    // Data platforms
    'cloud.getdbt.com': 'dbt',
    'getdbt.com': 'dbt',
    'dbt.com': 'dbt',
    'snowflake.com': 'snowflake',
    'snowflakecomputing.com': 'snowflake',
    // Common MCP integrations
    'github.com': 'github',
    'slack.com': 'slack',
    'notion.so': 'notion',
    'notion.com': 'notion',
    'glean.com': 'glean',
};

/**
 * Extracts a standard platform name from a URL for analytics tracking.
 * Returns the platform name (e.g., 'snowflake', 'github', 'dbt') or the hostname if unknown.
 */
export function extractPlatformFromUrl(url: string | undefined | null): string | undefined {
    if (!url) return undefined;

    const hostname = extractHostname(url);
    if (!hostname) return undefined;

    // Check for known platform domains
    const knownDomain = Object.keys(DOMAIN_TO_PLATFORM).find((domain) => hostname.includes(domain));
    if (knownDomain) {
        return DOMAIN_TO_PLATFORM[knownDomain];
    }

    // Return the main domain as fallback (e.g., 'example' from 'api.example.com')
    const parts = hostname.split('.');
    if (parts.length >= 2) {
        // Get the second-to-last part (main domain name)
        return parts[parts.length - 2];
    }

    return hostname;
}

/**
 * Gets the logo URL for an MCP server.
 * Priority:
 * 1. Match URL domain to KNOWN_MCP_LOGOS (most reliable)
 * 2. Match display name to PLATFORM_URN_TO_LOGO (data platforms)
 * 3. Match display name to KNOWN_MCP_LOGOS keywords (MCP integrations)
 * 4. Return null (component should show fallback icon)
 *
 * Note: URL matching takes priority over display name to avoid false matches
 * when plugin names happen to contain keywords like "dbt" or "github".
 */
export function getPluginLogoUrl(
    displayName: string | undefined | null,
    url: string | undefined | null,
): string | null {
    // First, try to match URL to known domains (most reliable)
    if (url) {
        const hostname = extractHostname(url);
        if (hostname) {
            const knownDomain = Object.keys(KNOWN_MCP_LOGOS).find((domain) => hostname.includes(domain));
            if (knownDomain) {
                return KNOWN_MCP_LOGOS[knownDomain];
            }
        }
    }

    // Then, try to match display name to known data platforms
    if (displayName) {
        const platformLogo = getLogoFromDisplayName(displayName);
        if (platformLogo) return platformLogo;

        // Finally, try to match display name to MCP integration keywords
        const mcpLogo = getLogoFromDisplayNameMcp(displayName);
        if (mcpLogo) return mcpLogo;
    }

    // No match found - return null, component will show fallback icon
    return null;
}

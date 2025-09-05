/**
 * Storage-free OAuth state generation utilities for DataHub Teams integration.
 *
 * This module provides secure OAuth state parameter creation that includes
 * all necessary context without requiring browser storage.
 */

export type OAuthFlowType = 'platform_integration' | 'personal_notifications' | 'admin_setup';

export interface OAuthStateParams {
    /** DataHub instance URL for routing */
    url: string;
    /** Type of OAuth flow */
    flowType: OAuthFlowType;
    /** Microsoft tenant ID */
    tenantId?: string;
    /** DataHub user URN (for personal flows) */
    userUrn?: string;
    /** Path to redirect after OAuth completion */
    redirectPath?: string;
    /** Original path user came from */
    originPath?: string;
    /** Additional client-specific metadata */
    clientMetadata?: Record<string, any>;
}

/**
 * Generate cryptographically secure OAuth state parameter.
 *
 * This function creates a storage-free OAuth state that contains all necessary
 * routing and context information, eliminating the need for browser storage.
 *
 * @param params OAuth state parameters
 * @returns Base64-encoded state parameter safe for URLs
 */
export function createOAuthState(params: OAuthStateParams): string {
    // Generate cryptographically secure nonce (256 bits)
    const nonceArray = new Uint8Array(32);
    crypto.getRandomValues(nonceArray);
    const nonce = Array.from(nonceArray, (b) => b.toString(16).padStart(2, '0')).join('');

    // Create V1 state object with all context
    const stateData = {
        // Core routing (required)
        url: params.url,
        nonce,

        // Security metadata
        timestamp: Math.floor(Date.now() / 1000), // Unix timestamp
        version: '1',

        // Flow context (replaces session storage)
        flow_type: params.flowType,

        // Routing context
        tenant_id: params.tenantId,
        user_urn: params.userUrn,

        // Navigation context
        redirect_path: params.redirectPath,
        origin_path: params.originPath,

        // Client context
        client_metadata: params.clientMetadata,
    };

    // Remove null/undefined values to keep state parameter smaller
    const cleanedState = Object.fromEntries(
        Object.entries(stateData).filter(([_, value]) => value != null && value !== undefined),
    );

    // Create deterministic JSON string and base64 encode (URL-safe, no padding)
    const jsonStr = JSON.stringify(cleanedState, Object.keys(cleanedState).sort());
    const encoded = btoa(jsonStr).replace(/=/g, '');

    console.log(`✅ OAuth state created: nonce=${nonce.substring(0, 8)}..., flow=${params.flowType}`);
    return encoded;
}

/**
 * Build complete Microsoft OAuth authorization URL.
 *
 * @param state Base64-encoded state parameter
 * @param oauthConfig OAuth configuration from GraphQL
 * @param tenantId Microsoft tenant ID (use 'common' for multi-tenant)
 * @returns Complete OAuth authorization URL
 */
export function buildMicrosoftOAuthUrl(
    state: string,
    oauthConfig: {
        appId: string;
        redirectUri: string;
        scopes: string;
        baseAuthUrl: string;
    },
    tenantId = 'common',
): string {
    const params = new URLSearchParams({
        client_id: oauthConfig.appId,
        response_type: 'code',
        redirect_uri: oauthConfig.redirectUri,
        scope: oauthConfig.scopes,
        state,
        response_mode: 'query',
    });

    return `${oauthConfig.baseAuthUrl}/${tenantId}/oauth2/v2.0/authorize?${params.toString()}`;
}

/**
 * Create OAuth state and build complete Microsoft OAuth URL in one step.
 *
 * @param stateParams OAuth state parameters
 * @param oauthConfig OAuth configuration from GraphQL
 * @param tenantId Microsoft tenant ID (use 'common' for multi-tenant)
 * @returns Complete OAuth authorization URL
 */
export function createOAuthUrl(
    stateParams: OAuthStateParams,
    oauthConfig: {
        appId: string;
        redirectUri: string;
        scopes: string;
        baseAuthUrl: string;
    },
    tenantId = 'common',
): string {
    const state = createOAuthState(stateParams);
    return buildMicrosoftOAuthUrl(state, oauthConfig, tenantId);
}

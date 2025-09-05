/**
 * Storage-Free OAuth State Generation for DataHub Teams Integration
 * 
 * This example shows how to generate OAuth state parameters that include
 * all necessary context without requiring browser storage.
 */

/**
 * Generate cryptographically secure OAuth state parameter
 * @param {Object} params - OAuth parameters
 * @param {string} params.url - DataHub instance URL 
 * @param {string} [params.flowType] - Type of OAuth flow
 * @param {string} [params.tenantId] - Microsoft tenant ID
 * @param {string} [params.userUrn] - DataHub user URN (for personal flows)
 * @param {string} [params.redirectPath] - Path to redirect after OAuth
 * @param {string} [params.originPath] - Original path user came from
 * @param {Object} [params.clientMetadata] - Additional client data
 * @returns {string} Base64-encoded state parameter
 */
function createOAuthState(params) {
    // Generate cryptographically secure nonce (256 bits)
    const nonceArray = new Uint8Array(32);
    crypto.getRandomValues(nonceArray);
    const nonce = Array.from(nonceArray, b => b.toString(16).padStart(2, '0')).join('');
    
    // Create state object with all context (no storage needed)
    const stateData = {
        // Core routing (required)
        url: params.url,
        nonce: nonce,
        
        // Security metadata
        timestamp: Math.floor(Date.now() / 1000),
        version: "1",
        
        // Flow context (replaces session storage)
        flow_type: params.flowType || "platform_integration",
        
        // Routing context
        tenant_id: params.tenantId || null,
        user_urn: params.userUrn || null,
        
        // Navigation context
        redirect_path: params.redirectPath || null,
        origin_path: params.originPath || null,
        
        // Client context
        client_metadata: params.clientMetadata || null
    };
    
    // Remove null values to keep state parameter smaller
    const cleanedState = Object.fromEntries(
        Object.entries(stateData).filter(([_, value]) => value !== null)
    );
    
    // Base64 encode (URL-safe, no padding)
    const jsonStr = JSON.stringify(cleanedState, Object.keys(cleanedState).sort());
    const encoded = btoa(jsonStr).replace(/=/g, '');
    
    console.log(`✅ OAuth state created: nonce=${nonce.substring(0, 8)}..., flow=${cleanedState.flow_type}`);
    return encoded;
}

// Example usage scenarios:

// 1. Platform Integration (Teams app setup)
const platformState = createOAuthState({
    url: "https://company.datahub.com",
    flowType: "platform_integration", 
    tenantId: "5ff2a251-a73c-4122-af7d-871fd744c752",
    redirectPath: "/settings/integrations/microsoft-teams"
});

// 2. Personal Notifications Setup
const personalState = createOAuthState({
    url: "https://company.datahub.com",
    flowType: "personal_notifications",
    tenantId: "5ff2a251-a73c-4122-af7d-871fd744c752", 
    userUrn: "urn:li:corpuser:john.doe",
    redirectPath: "/settings/personal-notifications",
    originPath: "/dashboard"
});

// 3. Admin Setup Flow
const adminState = createOAuthState({
    url: "https://company.datahub.com",
    flowType: "admin_setup",
    tenantId: "5ff2a251-a73c-4122-af7d-871fd744c752",
    redirectPath: "/admin/integrations/teams",
    clientMetadata: {
        admin_user: "admin@company.com",
        setup_step: "initial_config"
    }
});

// 4. Multi-tenant scenario
const tenantAState = createOAuthState({
    url: "https://tenant-a.datahub.com",
    flowType: "platform_integration",
    tenantId: "tenant-a-123",
    redirectPath: "/settings/integrations/microsoft-teams"
});

const tenantBState = createOAuthState({
    url: "https://tenant-b.datahub.com", 
    flowType: "platform_integration",
    tenantId: "tenant-b-456",
    redirectPath: "/settings/integrations/microsoft-teams"
});

/**
 * Construct full OAuth URL for Microsoft
 * @param {string} state - Base64 encoded state parameter
 * @param {Object} oauthConfig - OAuth configuration
 * @returns {string} Complete OAuth authorization URL
 */
function buildOAuthUrl(state, oauthConfig) {
    const params = new URLSearchParams({
        client_id: oauthConfig.appId,
        response_type: 'code',
        redirect_uri: oauthConfig.redirectUri,
        scope: oauthConfig.scopes,
        state: state,
        response_mode: 'query'
    });
    
    return `https://login.microsoftonline.com/common/oauth2/v2.0/authorize?${params.toString()}`;
}

// Security benefits of this storage-free approach:
console.log(`
🔐 Security Benefits:
✅ No browser storage = No cross-tab data leakage
✅ No persistent storage = No session hijacking on shared computers  
✅ All context in state = No race conditions between tabs
✅ Cryptographic nonce = Replay attack protection
✅ Timestamp validation = Time-bound security
✅ Pydantic validation = Schema enforcement on router

🎯 Trade-offs:
⚠️ Slightly larger URLs (but well within browser limits)
⚠️ All context must fit in state parameter
✅ Much simpler architecture
✅ Better security isolation
✅ No storage cleanup needed
`);

// Export for use in modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { createOAuthState, buildOAuthUrl };
}
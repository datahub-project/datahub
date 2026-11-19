package com.linkedin.gms.factory.policycache;

/**
 * Present only when GMS runs with POLICY_CACHE_BROADCAST_ENABLED=true (see {@link
 * PolicyChangeReceiverFactory}). The authorizer factory is shared with the standalone MCE consumer,
 * which does not scan this package and so cannot receive or send change signals: the broadcast flag
 * must turn on immediate policy updates only where this bean exists.
 */
public final class PolicyChangeBroadcast {}

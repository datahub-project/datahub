// This is disabled until we have a proper implementation
export const ENABLE_UPSTREAM_NOTIFICATIONS = false;

export const SubscriberTypes = {
    PERSONAL: 'personal',
    GROUP: 'group',
} as const;

export type SubscriberType = typeof SubscriberTypes[keyof typeof SubscriberTypes];

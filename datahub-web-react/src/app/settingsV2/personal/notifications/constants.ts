// This is disabled until we have a proper implementation
export const ENABLE_UPSTREAM_NOTIFICATIONS = false;

export const ActorTypes = {
    PERSONAL: 'personal',
    GROUP: 'group',
} as const;

export type ActorType = typeof ActorTypes[keyof typeof ActorTypes];

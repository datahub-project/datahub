export interface ReloadableContextType {
    reloadByKeyType: (keysTypes: string[], delayMs?: number) => void;
    markAsReloaded: (keyType: string, keyId?: string, delayMs?: number) => void;
    shouldBeReloaded: (keyType: string, keyId?: string) => boolean;
    bypassCacheForUrn: (urn: string) => void;
    shouldBypassCache: (urn: string) => boolean;
    clearCacheBypass: (urn: string) => void;
}

export enum ReloadableKeyTypeNamespace {
    MODULE = 'MODULE',
    STRUCTURED_PROPERTY = 'STRUCTURED_PROPERTY',
}

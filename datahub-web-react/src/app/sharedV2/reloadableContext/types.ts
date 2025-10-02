export interface ReloadableContextType {
    reloadByKeyType: (keysTypes: string[], delayMs?: number) => void;
    reloaded: (keyType: string, keyId?: string, delayMs?: number) => void;
    shouldBeReloaded: (keyType: string, keyId?: string) => boolean;
}

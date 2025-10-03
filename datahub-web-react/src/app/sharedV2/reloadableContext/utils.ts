export const KEY_SEPARATOR = '|';

export const getReloadableKey = (keyType: string, entryId?: string) => `${keyType}${KEY_SEPARATOR}${entryId ?? ''}`;

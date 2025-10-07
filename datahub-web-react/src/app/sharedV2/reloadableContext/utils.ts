import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';

export const KEY_SEPARATOR = '|';
export const NAMESPACE_SEPARATOR = '>';

export const getReloadableKey = (keyType: string, entryId?: string) => `${keyType}${KEY_SEPARATOR}${entryId ?? ''}`;

export const getReloadableKeyType = (typeNamespace: ReloadableKeyTypeNamespace, typeName: string) =>
    `${typeNamespace}${NAMESPACE_SEPARATOR}${typeName}`;

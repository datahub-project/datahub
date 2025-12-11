/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export interface ReloadableContextType {
    reloadByKeyType: (keysTypes: string[], delayMs?: number) => void;
    markAsReloaded: (keyType: string, keyId?: string, delayMs?: number) => void;
    shouldBeReloaded: (keyType: string, keyId?: string) => boolean;
}

export enum ReloadableKeyTypeNamespace {
    MODULE = 'MODULE',
    STRUCTURED_PROPERTY = 'STRUCTURED_PROPERTY',
}

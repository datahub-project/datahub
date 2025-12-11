/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';

export const KEY_SEPARATOR = '|';
export const NAMESPACE_SEPARATOR = '>';

export const getReloadableKey = (keyType: string, entryId?: string) => `${keyType}${KEY_SEPARATOR}${entryId ?? ''}`;

export const getReloadableKeyType = (typeNamespace: ReloadableKeyTypeNamespace, typeName: string) =>
    `${typeNamespace}${NAMESPACE_SEPARATOR}${typeName}`;

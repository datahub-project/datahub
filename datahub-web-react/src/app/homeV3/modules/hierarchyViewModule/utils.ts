/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { extractTypeFromUrn } from '@app/entity/shared/utils';
import {
    ASSET_TYPE_DOMAINS,
    ASSET_TYPE_GLOSSARY,
    DEFAULT_ASSET_TYPE,
} from '@app/homeV3/modules/hierarchyViewModule/constants';
import { AssetType } from '@app/homeV3/modules/hierarchyViewModule/types';

import { EntityType } from '@types';

export function isUrnDomainAssetType(urn: string): boolean {
    const entityType = extractTypeFromUrn(urn);
    return entityType === EntityType.Domain;
}

export function isUrnGlossaryAssetType(urn: string): boolean {
    const entityType = extractTypeFromUrn(urn);
    return [EntityType.GlossaryNode, EntityType.GlossaryTerm].includes(entityType);
}

export function getAssetTypeFromAssetUrns(urns?: string[]): AssetType {
    if (!urns?.length) return DEFAULT_ASSET_TYPE;

    const urn = urns[0];

    if (isUrnDomainAssetType(urn)) return ASSET_TYPE_DOMAINS;
    if (isUrnGlossaryAssetType(urn)) return ASSET_TYPE_GLOSSARY;

    console.warn('Unsupportable urn:', urn);
    return DEFAULT_ASSET_TYPE;
}

export function filterAssetUrnsByAssetType(urns: string[] | undefined, assetType: AssetType): string[] {
    if (!urns?.length) return [];

    if (assetType === ASSET_TYPE_DOMAINS) {
        return urns.filter((urn) => isUrnDomainAssetType(urn));
    }

    if (assetType === ASSET_TYPE_GLOSSARY) {
        return urns.filter((urn) => isUrnGlossaryAssetType(urn));
    }

    console.warn('Unsupportable assetType:', assetType);
    return [];
}

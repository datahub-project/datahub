import { extractTypeFromUrn } from '@app/entity/shared/utils';

import { EntityType } from '@types';

import { ASSET_TYPE_DOMAINS, ASSET_TYPE_GLOSSARY, DEFAULT_ASSET_TYPE } from './constants';
import { AssetType } from './types';

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

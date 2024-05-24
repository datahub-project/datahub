import { GlobalTags, Owner } from '../../types.generated';
import { EntityCapabilityType } from '../entityV2/Entity';

export function getUniqueOwners(owners?: Owner[] | null) {
    const uniqueOwnerUrns = new Set();
    return owners?.filter((owner) => !uniqueOwnerUrns.has(owner.owner.urn) && uniqueOwnerUrns.add(owner.owner.urn));
}

export const entityHasCapability = (
    capabilities: Set<EntityCapabilityType>,
    capabilityToCheck: EntityCapabilityType,
): boolean => capabilities.has(capabilityToCheck);

export const getHighlightedTag = (tags?: GlobalTags) => {
    if (tags && tags.tags?.length) {
        if (tags?.tags[0].tag.properties) return tags?.tags[0].tag.properties.name;
        return tags?.tags[0].tag.name;
    }
    return '';
};

export const isNullOrUndefined = (value: any) => {
    return value === null || value === undefined;
};

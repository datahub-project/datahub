import { Owner } from '../../types.generated';
import { EntityCapabilityType } from '../entityV2/Entity';

export function getUniqueOwners(owners?: Owner[] | null) {
    const uniqueOwnerUrns = new Set();
    return owners?.filter((owner) => !uniqueOwnerUrns.has(owner.owner.urn) && uniqueOwnerUrns.add(owner.owner.urn));
}

export const entityHasCapability = (
    capabilities: Set<EntityCapabilityType>,
    capabilityToCheck: EntityCapabilityType
  ): boolean => capabilities.has(capabilityToCheck);
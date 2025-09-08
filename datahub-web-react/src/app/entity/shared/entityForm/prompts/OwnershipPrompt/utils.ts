import { GenericEntityProperties } from '@app/entity/shared/types';
import { FormPrompt, PromptCardinality } from '@src/types.generated';

// Get owners initial owners with the same initial type urn
export const getDefaultOwnerEntities = (
    entityData: GenericEntityProperties | null,
    prompt: FormPrompt,
    initialOwnershipTypeUrn: string | undefined,
) => {
    if (!initialOwnershipTypeUrn) return [];
    const allowedOwnerUrns = prompt.ownershipParams?.allowedOwners?.map((entity) => entity.urn) || [];

    const filteredOwners =
        entityData?.ownership?.owners
            ?.filter((owner) => owner.ownershipType?.urn === initialOwnershipTypeUrn)
            ?.filter((owner) => (allowedOwnerUrns.length > 0 ? allowedOwnerUrns.includes(owner.owner.urn) : true))
            ?.map((owner) => owner.owner) || [];

    const cardinality = prompt.ownershipParams?.cardinality;
    if (cardinality === PromptCardinality.Single) {
        return filteredOwners.length > 0 ? [filteredOwners[0]] : [];
    }

    return filteredOwners;
};

// Get default ownership type urn as the first in the allowed types or the first type of an existing owner
export const getDefaultOwnershipTypeUrn = (entityData: GenericEntityProperties | null, prompt: FormPrompt) => {
    const initialOwners = entityData?.ownership?.owners;
    const distinctOwnershipTypes = [...new Set(initialOwners?.map((owner) => owner.ownershipType?.urn))];

    if (prompt.ownershipParams?.allowedOwnershipTypes && prompt.ownershipParams.allowedOwnershipTypes.length > 0) {
        return prompt.ownershipParams.allowedOwnershipTypes[0].urn;
    }
    if (prompt.ownershipParams?.allowedOwners && prompt.ownershipParams.allowedOwners.length > 0) {
        const allowedOwnerUrns = prompt.ownershipParams.allowedOwners.map((entity) => entity.urn);
        const existingAllowedOwners =
            initialOwners?.filter((owner) => owner.ownershipType?.urn && allowedOwnerUrns.includes(owner.owner.urn)) ||
            [];
        return existingAllowedOwners.length > 0 ? existingAllowedOwners[0].ownershipType?.urn : undefined;
    }
    if (distinctOwnershipTypes.length > 0) {
        return distinctOwnershipTypes[0];
    }
    return undefined;
};

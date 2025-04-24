import { GenericEntityProperties } from '@app/entity/shared/types';
import { FormPrompt, PromptCardinality } from '@src/types.generated';

// Get default value of owners from the asset based on different conditions to show in the form
export const getDefaultOwnerEntities = (entityData: GenericEntityProperties | null, prompt: FormPrompt) => {
    const allowedOwnershipTypeUrns = prompt.ownershipParams?.allowedOwnershipTypes?.map((type) => type.urn);
    const filteredOwners = entityData?.ownership?.owners?.filter((owner) =>
        allowedOwnershipTypeUrns?.includes(owner.ownershipType?.urn || ''),
    );
    const entityOwners =
        allowedOwnershipTypeUrns && allowedOwnershipTypeUrns?.length > 0
            ? filteredOwners?.map((owner) => owner.owner)
            : entityData?.ownership?.owners?.map((owner) => owner.owner);
    const cardinality = prompt.ownershipParams?.cardinality;
    const allowedOwners = prompt.ownershipParams?.allowedOwners;
    const allowedOwnersUrns = allowedOwners?.map((owner) => owner.urn);
    const areOwnersRestricted = (allowedOwners && allowedOwners?.length > 0) ?? false;

    if (entityOwners && entityOwners.length > 0) {
        if (cardinality === PromptCardinality.Multiple && !areOwnersRestricted) {
            return entityOwners;
        }
        if (cardinality === PromptCardinality.Multiple && areOwnersRestricted) {
            return entityOwners.filter((owner) => allowedOwnersUrns?.includes(owner.urn));
        }
        if (cardinality === PromptCardinality.Single && !areOwnersRestricted) {
            return entityOwners[0] ? [entityOwners[0]] : [];
        }
        if (cardinality === PromptCardinality.Single && areOwnersRestricted) {
            const defaultOwners = entityOwners.filter((owner) => allowedOwnersUrns?.includes(owner.urn));
            return defaultOwners.length > 0 ? [defaultOwners[0]] : [];
        }
    }
    return [];
};

// Get default ownership type urn if cardinality is single or only one ownership type is allowed
export const getDefaultOwnershipTypeUrn = (
    entityData: GenericEntityProperties | null,
    prompt: FormPrompt,
    initialValues: string[],
) => {
    const initialOwners = entityData?.ownership?.owners?.filter((owner) => initialValues.includes(owner.owner.urn));
    const distinctOwnershipTypes = [...new Set(initialOwners?.map((owner) => owner.ownershipType?.urn))];

    if (prompt.ownershipParams?.allowedOwnershipTypes && prompt.ownershipParams.allowedOwnershipTypes.length === 1) {
        return prompt.ownershipParams.allowedOwnershipTypes[0].urn;
    }
    if (prompt.ownershipParams?.cardinality === PromptCardinality.Single) {
        return entityData?.ownership?.owners?.find((owner) => owner.owner.urn === initialValues[0])?.ownershipType?.urn;
    }
    if (distinctOwnershipTypes.length === 1) {
        return distinctOwnershipTypes[0];
    }
    return undefined;
};

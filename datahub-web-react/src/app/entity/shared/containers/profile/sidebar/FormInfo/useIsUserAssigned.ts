import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { getFormAssociations } from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';

import { FormAssociation, Owner } from '@types';

function isOwnerWithType(
    owners: Owner[] | undefined,
    userUrn: string,
    ownershipTypeUrns: string[] | undefined,
): boolean {
    if (!owners) return false;

    const userOwnership = owners.find((owner) => owner.owner.urn === userUrn);
    if (!userOwnership) return false;

    if (!ownershipTypeUrns || ownershipTypeUrns.length === 0) {
        return true;
    }

    return !!userOwnership.ownershipType && ownershipTypeUrns.includes(userOwnership.ownershipType.urn);
}

export function isAssignedToForm(formAssociation: FormAssociation, owners: Owner[] | undefined, userUrn: string) {
    const { isAssignedToMe, owners: isAssignedToOwners, ownershipTypes } = formAssociation.form.info.actors;

    if (isAssignedToMe) {
        return true;
    }

    if (isAssignedToOwners) {
        const ownershipTypeUrns = ownershipTypes?.map((type) => type.urn);
        return isOwnerWithType(owners, userUrn, ownershipTypeUrns);
    }

    return false;
}

export default function useIsUserAssigned(formUrn?: string) {
    const { entityData } = useEntityData();
    const owners = entityData?.ownership?.owners;
    const { user: loggedInUser } = useUserContext();
    const userUrn = loggedInUser?.urn || '';

    const formAssociations = getFormAssociations(entityData);
    if (formUrn) {
        const formAssociation = formAssociations.find((association) => association.form.urn === formUrn);
        return formAssociation ? isAssignedToForm(formAssociation, owners, userUrn) : false;
    }
    return formAssociations.some((formAssociation) => isAssignedToForm(formAssociation, owners, userUrn));
}

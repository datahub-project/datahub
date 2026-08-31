import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { getFormAssociations } from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';

import { FormAssociation, Maybe, Owner } from '@types';

function isOwnerWithType(
    owners: Maybe<Owner[]> | undefined,
    userUrn: string,
    ownershipTypeUrns: string[] | undefined,
): boolean {
    if (!owners) return false;

    const userOwnerships = owners.filter((owner) => owner.owner.urn === userUrn);
    if (userOwnerships.length === 0) return false;

    if (!ownershipTypeUrns || ownershipTypeUrns.length === 0) {
        return true;
    }

    // A user can own the same entity under multiple ownership types, so the form is assigned if any
    // of the user's ownership types matches any of the form's specified ownership types.
    return userOwnerships.some(
        (ownership) => !!ownership.ownershipType && ownershipTypeUrns.includes(ownership.ownershipType.urn),
    );
}

export function isAssignedToForm(
    formAssociation: FormAssociation,
    owners: Maybe<Owner[]> | undefined,
    userUrn: string,
) {
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

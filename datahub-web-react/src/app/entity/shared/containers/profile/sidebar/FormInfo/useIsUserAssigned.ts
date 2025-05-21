import { useUserContext } from '@app/context/useUserContext';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { getFormAssociations } from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';

import { FormAssociation } from '@types';

export function isAssignedToForm(formAssociation: FormAssociation, isUserAnOwner: boolean) {
    const { isAssignedToMe, owners: isAssignedToOwners } = formAssociation.form.info.actors;
    return isAssignedToMe || (isAssignedToOwners && isUserAnOwner);
}

// returns true if this user is assigned (explicitly or by ownership) to a given form or any forms on this entity
export default function useIsUserAssigned(formUrn?: string) {
    const { entityData } = useEntityData();
    const owners = entityData?.ownership?.owners;
    const { user: loggedInUser } = useUserContext();
    const isUserAnOwner = !!owners?.find((owner) => owner.owner.urn === loggedInUser?.urn);

    const formAssociations = getFormAssociations(entityData);
    if (formUrn) {
        const formAssociation = formAssociations.find((association) => association.form.urn === formUrn);
        return formAssociation ? isAssignedToForm(formAssociation, isUserAnOwner) : false;
    }
    return formAssociations.some((formAssociation) => isAssignedToForm(formAssociation, isUserAnOwner));
}

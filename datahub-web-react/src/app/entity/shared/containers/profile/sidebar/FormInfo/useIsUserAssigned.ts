import { FormAssociation } from '../../../../../../../types.generated';
import { useUserContext } from '../../../../../../context/useUserContext';
import { useEntityData } from '../../../../EntityContext';
import { getFormAssociations } from './utils';

export function isAssignedToForm(formAssociation: FormAssociation, isUserAnOwner: boolean) {
    const { isAssignedToMe, owners: isAssignedToOwners } = formAssociation.form.info.actors;
    return isAssignedToMe || (isAssignedToOwners && isUserAnOwner);
}

// returns true if this user is assigned (explicitly or by ownership) to a given form or any forms on this entity
export default function useIsUserAssigned(formUrn?: string) {
    const { entityData } = useEntityData();
    const owners = entityData?.ownership?.owners;
    const { user: loggedInUser, userGroups } = useUserContext();
    const userGroupUrns = userGroups?.relationships.map((rel) => rel.entity?.urn);
    const isUserAnOwner = !!owners?.find((owner) => owner.owner.urn === loggedInUser?.urn);
    const isUserInOwnerGroup = !!owners?.find((owner) => userGroupUrns?.includes(owner.owner.urn));
    const isOwner = isUserAnOwner || isUserInOwnerGroup;

    const formAssociations = getFormAssociations(entityData);
    if (formUrn) {
        const formAssociation = formAssociations.find((association) => association.form.urn === formUrn);
        return formAssociation ? isAssignedToForm(formAssociation, isOwner) : false;
    }
    return formAssociations.some((formAssociation) => isAssignedToForm(formAssociation, isOwner));
}

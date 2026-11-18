import { useUserContext } from '@app/context/useUserContext';
import { getActorPoliciesVisibility } from '@app/permissions/policy/actorPolicies/actorPolicies.utils';

export default function useActorPoliciesVisibility() {
    return getActorPoliciesVisibility(useUserContext().platformPrivileges);
}

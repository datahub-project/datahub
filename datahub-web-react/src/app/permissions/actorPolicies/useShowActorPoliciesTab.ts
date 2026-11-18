import { useUserContext } from '@app/context/useUserContext';
import { useAppConfig } from '@app/useAppConfig';

/**
 * Tab visibility only; the server enforces access. Mirrors its rules: policy admins see any
 * profile, everyone else needs the flag and must be viewing themselves or a group they belong to.
 */
export function useShowActorPoliciesTab({
    profileUrn,
    isGroup,
    isMember,
}: {
    profileUrn: string;
    isGroup: boolean;
    isMember: boolean;
}): boolean {
    const { urn: myUrn, platformPrivileges } = useUserContext();
    const { config } = useAppConfig();

    if (platformPrivileges?.managePolicies) return true;
    if (!config.featureFlags.actorPoliciesSelfServiceEnabled) return false;
    return isGroup ? isMember : profileUrn === myUrn;
}

import { useUserContext } from '@app/context/useUserContext';
import useGetUserGroupUrns from '@app/entityV2/user/useGetUserGroupUrns';
import { useAppConfig } from '@app/useAppConfig';

/**
 * Tab visibility only; the server enforces access. Mirrors its rules: policy admins see any
 * profile, everyone else needs the flag and must be viewing themselves or a group they belong to.
 */
export function useShowActorPoliciesTab({ profileUrn, isGroup }: { profileUrn: string; isGroup: boolean }): boolean {
    const { urn: myUrn, platformPrivileges } = useUserContext();
    const { config } = useAppConfig();
    const { groupUrns, data } = useGetUserGroupUrns(isGroup ? myUrn || undefined : undefined);

    if (platformPrivileges?.managePolicies) return true;
    if (!config.featureFlags.actorPoliciesSelfServiceEnabled) return false;
    if (!isGroup) return profileUrn === myUrn;
    // Only the first page of my groups is fetched. If there are more, I may be a member; the server
    // decides, and a denial renders as the empty state.
    const total = data?.corpUser?.relationships?.total ?? 0;
    return groupUrns.includes(profileUrn) || total > groupUrns.length;
}

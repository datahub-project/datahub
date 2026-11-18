import { RESOURCE_TYPE, TYPE } from '@app/permissions/policy/constants';
import {
    convertLegacyResourceFilter,
    getFieldValues,
    mapResourceTypeToDisplayName,
} from '@app/permissions/policy/policyUtils';

import { ActorPolicyFieldsFragment } from '@graphql/actorPolicies.generated';
import { Entity, Policy, PolicyType, ResourcePrivileges } from '@types';

/**
 * Labels describing what a policy applies to: resource-type display names, then the specific entities
 * (resolved to names where the query resolves them). An empty list means every resource; null means resources
 * do not apply (platform policies).
 */
export const resourceScopeLabels = (
    policy: ActorPolicyFieldsFragment,
    resourcePrivileges: ResourcePrivileges[],
    entityName: (entity: Entity) => string,
): string[] | null => {
    if (policy.type === PolicyType.Platform) {
        return null;
    }
    const criteria = convertLegacyResourceFilter(policy.resources as Policy['resources'])?.filter?.criteria ?? [];
    const typeValues = getFieldValues({ criteria }, TYPE, RESOURCE_TYPE);
    const types = typeValues.map((v) => mapResourceTypeToDisplayName(v.value, resourcePrivileges) || v.value);
    const entities = criteria
        .filter((criterion) => criterion.field !== TYPE && criterion.field !== RESOURCE_TYPE)
        .flatMap((criterion) => criterion.values.map((v) => (v.entity ? entityName(v.entity) : v.value)));
    return [...types, ...entities];
};

// Safe because the fragment mirrors the selection of listPolicies, which is what PolicyDetailsModal renders;
// the DataHubPolicy entity has a different shape, so it cannot be used here.
export const toPolicy = (policy: ActorPolicyFieldsFragment) =>
    ({ ...policy, editable: false }) as unknown as Omit<Policy, 'urn'>;

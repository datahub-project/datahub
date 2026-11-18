import { TFunction } from 'i18next';

import { RESOURCE_TYPE, TYPE } from '@app/permissions/policy/constants';
import {
    convertLegacyResourceFilter,
    getConditionLabel,
    mapResourceTypeToDisplayName,
} from '@app/permissions/policy/policyUtils';

import { ActorPolicyFieldsFragment } from '@graphql/actorPolicies.generated';
import { Entity, Policy, PolicyMatchCondition, PolicyType, ResourcePrivileges } from '@types';

/**
 * Labels describing what a policy applies to: resource-type display names, then the specific entities
 * (resolved to names where the query resolves them). A criterion that excludes or matches by prefix is
 * labelled with its condition, so it does not read as included. An empty list means every resource; null
 * means resources do not apply (platform policies). `t` must be bound to settings.permissions.
 */
export const resourceScopeLabels = (
    policy: ActorPolicyFieldsFragment,
    resourcePrivileges: ResourcePrivileges[],
    entityName: (entity: Entity) => string,
    t: TFunction,
): string[] | null => {
    if (policy.type === PolicyType.Platform) {
        return null;
    }
    const criteria = convertLegacyResourceFilter(policy.resources as Policy['resources'])?.filter?.criteria ?? [];
    const isType = (field: string) => field === TYPE || field === RESOURCE_TYPE;
    const withCondition = (label: string, condition?: PolicyMatchCondition | null) =>
        condition && condition !== PolicyMatchCondition.Equals ? `${getConditionLabel(condition, t)} ${label}` : label;
    // TYPE before RESOURCE_TYPE: only one is used, as in the policy editor.
    const typeCriterion =
        criteria.find((criterion) => criterion.field === TYPE) ??
        criteria.find((criterion) => criterion.field === RESOURCE_TYPE);
    const types = (typeCriterion?.values ?? []).map((v) =>
        withCondition(mapResourceTypeToDisplayName(v.value, resourcePrivileges) || v.value, typeCriterion?.condition),
    );
    const entities = criteria
        .filter((criterion) => !isType(criterion.field))
        .flatMap((criterion) =>
            criterion.values.map((v) => withCondition(v.entity ? entityName(v.entity) : v.value, criterion.condition)),
        );
    return [...types, ...entities];
};

// Safe because the fragment mirrors the selection of listPolicies, which is what PolicyDetailsModal renders;
// the DataHubPolicy entity has a different shape, so it cannot be used here.
export const toPolicy = (policy: ActorPolicyFieldsFragment) =>
    ({ ...policy, editable: false }) as unknown as Omit<Policy, 'urn'>;

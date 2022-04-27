import { Maybe } from 'graphql/jsutils/Maybe';
import {
    PolicyMatchCondition,
    PolicyMatchCriterion,
    PolicyMatchCriterionValue,
    PolicyMatchFilter,
    PolicyState,
    PolicyType,
    Privilege,
    ResourceFilter,
    ResourcePrivileges,
} from '../../types.generated';

export const EMPTY_POLICY = {
    type: PolicyType.Metadata,
    name: '',
    description: '',
    state: PolicyState.Active,
    privileges: new Array<string>(),
    resources: {
        filter: {
            criteria: [],
        },
    },
    actors: {
        users: new Array<string>(),
        groups: new Array<string>(),
        allUsers: false,
        allGroups: false,
        resourceOwners: false,
    },
    editable: true,
};

/**
 * This is used for search - it allows you to map an asset resource to a search type.
 */
export const mapResourceTypeToEntityType = (resourceType: string, resourcePrivilegesArr: Array<ResourcePrivileges>) => {
    const resourcePrivileges = resourcePrivilegesArr.filter((privs) => privs.resourceType === resourceType);
    if (resourcePrivileges.length === 1) {
        return resourcePrivileges[0].entityType;
    }
    return null;
};

export const mapResourceTypeToDisplayName = (
    resourceType: string,
    resourcePrivilegesArr: Array<ResourcePrivileges>,
) => {
    const resourcePrivileges = resourcePrivilegesArr.filter((privs) => privs.resourceType === resourceType);
    if (resourcePrivileges.length === 1) {
        return resourcePrivileges[0].resourceTypeDisplayName;
    }
    return '';
};

export const mapResourceTypeToPrivileges = (
    resourceTypes: string[],
    resourcePrivilegesArr: Array<ResourcePrivileges>,
) => {
    if (resourceTypes.length === 0) {
        return resourcePrivilegesArr.find((privs) => privs.resourceType === 'all')?.privileges || [];
    }
    // If input resource types are empty, consider all privileges
    const resourcePrivileges = resourcePrivilegesArr.filter((privs) => resourceTypes.includes(privs.resourceType));
    if (resourcePrivileges.length > 0) {
        const finalPrivileges: Privilege[] = [];
        const uniquePrivileges = new Set();
        resourcePrivileges.forEach((resourcePrivilege) => {
            resourcePrivilege.privileges.forEach((privilege) => {
                if (!uniquePrivileges.has(privilege.type)) {
                    uniquePrivileges.add(privilege.type);
                    finalPrivileges.push(privilege);
                }
            });
        });
        return finalPrivileges;
    }
    return [];
};

export const createCriterion = (
    resourceFieldType: string,
    fieldValues: Array<PolicyMatchCriterionValue>,
): PolicyMatchCriterion => ({
    field: resourceFieldType,
    values: fieldValues,
    condition: PolicyMatchCondition.Equals,
});

export const createCriterionValue = (value: string): PolicyMatchCriterionValue => ({ value });

export const createCriterionValueWithEntity = (value: string, entity): PolicyMatchCriterionValue => ({ value, entity });

export const convertLegacyResourceFilter = (resourceFilter: Maybe<ResourceFilter> | undefined) => {
    // If empty or filter is set, resource filter is valid so return itself
    if (!resourceFilter || resourceFilter.filter) {
        return resourceFilter;
    }
    const criteria = new Array<PolicyMatchCriterion>();
    if (resourceFilter.type) {
        criteria.push(createCriterion('RESOURCE_TYPE', [createCriterionValue(resourceFilter.type)]));
    }
    if (resourceFilter.resources && resourceFilter.resources.length > 0) {
        criteria.push(createCriterion('RESOURCE_URN', resourceFilter.resources.map(createCriterionValue)));
    }
    return {
        filter: {
            criteria,
        },
    };
};

export const getFieldValues = (filter: Maybe<PolicyMatchFilter> | undefined, resourceFieldType: string) => {
    return filter?.criteria?.find((criterion) => criterion.field === resourceFieldType)?.values || [];
};

export const setFieldValues = (
    filter: PolicyMatchFilter,
    resourceFieldType: string,
    fieldValues: Array<PolicyMatchCriterionValue>,
): PolicyMatchFilter => {
    const restCriteria = filter.criteria?.filter((criterion) => criterion.field !== resourceFieldType) || [];
    if (fieldValues.length === 0) {
        return { ...filter, criteria: restCriteria };
    }
    return { ...filter, criteria: [...restCriteria, createCriterion(resourceFieldType, fieldValues)] };
};

import { PolicyState, PolicyType, ResourcePrivileges } from '../../types.generated';

export const EMPTY_POLICY = {
    type: PolicyType.Metadata,
    name: '',
    description: '',
    state: PolicyState.Active,
    privileges: new Array<string>(),
    resources: {
        type: '',
        resources: new Array<string>(),
        allResources: false,
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

export const mapResourceTypeToPrivileges = (resourceType: string, resourcePrivilegesArr: Array<ResourcePrivileges>) => {
    const resourcePrivileges = resourcePrivilegesArr.filter((privs) => privs.resourceType === resourceType);
    if (resourcePrivileges.length === 1) {
        return resourcePrivileges[0].privileges;
    }
    return [];
};

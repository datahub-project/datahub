import { useCallback, useEffect, useMemo, useState } from 'react';

import { useListRolesQuery } from '@graphql/role.generated';
import { DataHubRole } from '@types';

function buildRoleSelectOptions(roles: Array<DataHubRole>, noRoleText: string) {
    const roleOptions = roles.map((role) => ({
        value: role.urn,
        label: role.name,
    }));

    const hasNoRole = roles.some((role) => role.name === noRoleText);

    if (!hasNoRole) {
        return [...roleOptions, { value: '', label: noRoleText }];
    }

    const filteredOptions = roleOptions.filter((option) => option.label !== noRoleText);
    const noRoleOption = roleOptions.find((option) => option.label === noRoleText);

    return noRoleOption ? [...filteredOptions, noRoleOption] : filteredOptions;
}

function getRoleFromUrn(roleUrn: string, rolesMap: Map<string, DataHubRole>): DataHubRole | undefined {
    return roleUrn === '' ? undefined : rolesMap.get(roleUrn);
}

export function useRoleManagement() {
    // Track if roles have been initialized to avoid resetting user selections
    const [rolesInitialized, setRolesInitialized] = useState<boolean>(false);
    const [selectedRole, setSelectedRole] = useState<DataHubRole>();
    const [emailInviteRole, setEmailInviteRole] = useState<DataHubRole>();

    const noRoleText = 'No Role';

    const { data: rolesData } = useListRolesQuery({
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                start: 0,
                count: 10,
            },
        },
    });

    const roles: Array<DataHubRole> = useMemo(() => {
        return rolesData?.listRoles?.roles || [];
    }, [rolesData?.listRoles?.roles]);

    const rolesMap: Map<string, DataHubRole> = useMemo(() => {
        const map = new Map();
        roles.forEach((role) => {
            map.set(role.urn, role);
        });
        return map;
    }, [roles]);

    // Find the Reader role as default (memoized to prevent infinite re-renders)
    const defaultReaderRole = useMemo(() => {
        return roles.find((role) => role.name === 'Reader');
    }, [roles]);

    // Shared role options
    const roleSelectOptions = useMemo(() => {
        return buildRoleSelectOptions(roles, noRoleText);
    }, [roles, noRoleText]);

    // Set default roles on first load only - Reader is the default, fallback to first role if Reader doesn't exist
    useEffect(() => {
        if (roles.length > 0 && !rolesInitialized) {
            const defaultRole = defaultReaderRole || roles[0]; // Prefer Reader, fallback to first role
            setSelectedRole(defaultRole);
            setEmailInviteRole(defaultRole);
            setRolesInitialized(true);
        }
    }, [roles, defaultReaderRole, rolesInitialized]);

    const onSelectRole = useCallback(
        (roleUrn: string) => {
            setSelectedRole(getRoleFromUrn(roleUrn, rolesMap));
        },
        [rolesMap],
    );

    const onSelectEmailInviteRole = useCallback(
        (roleUrn: string) => {
            setEmailInviteRole(getRoleFromUrn(roleUrn, rolesMap));
        },
        [rolesMap],
    );

    const resetRoles = useCallback(() => {
        setSelectedRole(undefined);
        setEmailInviteRole(undefined);
        setRolesInitialized(false);
    }, []);

    return {
        // State
        selectedRole,
        emailInviteRole,

        // Derived data
        roles,
        roleSelectOptions,
        noRoleText,

        // Handlers
        onSelectRole,
        onSelectEmailInviteRole,
        resetRoles,
    };
}

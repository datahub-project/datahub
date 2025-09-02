import { SimpleSelect } from '@components';
import React, { useMemo } from 'react';

import { useListRolesQuery } from '@graphql/role.generated';
import { DataHubRole } from '@types';

interface Props {
    selectedRole?: DataHubRole;
    onRoleSelect: (role: DataHubRole | undefined) => void;
    placeholder?: string;
    size?: 'sm' | 'md' | 'lg';
    width?: number | 'full' | 'fit-content';
    disabled?: boolean;
}

export default function SimpleSelectRole({
    selectedRole,
    onRoleSelect,
    placeholder = 'No Role',
    size = 'md',
    width = 'fit-content',
    disabled = false,
}: Props) {
    // Fetch roles for the dropdown
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

    const roleSelectOptions = useMemo(() => {
        const roleOptions = roles.map((role) => ({
            value: role.urn,
            label: role.name,
        }));

        // Add "No Role" option at the end
        return [...roleOptions, { value: '', label: placeholder }];
    }, [roles, placeholder]);

    const handleRoleSelect = (roleUrn: string) => {
        if (roleUrn === '') {
            onRoleSelect(undefined);
        } else {
            const selectedRoleFromList = roles.find((role) => role.urn === roleUrn);
            onRoleSelect(selectedRoleFromList);
        }
    };

    return (
        <SimpleSelect
            onUpdate={(values) => handleRoleSelect(values[0] || '')}
            options={roleSelectOptions}
            placeholder={placeholder}
            values={selectedRole?.urn ? [selectedRole.urn] : []}
            size={size}
            width={width}
            isDisabled={disabled}
            showClear={false}
        />
    );
}

import { Icon, SimpleSelect, Tooltip } from '@components';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { mapRoleToPhosphorIcon } from '@app/identity/user/PhosphorRoleUtils';

import { useListRolesQuery } from '@graphql/role.generated';
import { DataHubRole } from '@types';

const PlaceholderContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

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
    const [isDropdownOpen, setIsDropdownOpen] = useState(false);

    const placeholderWithIcon = (
        <PlaceholderContainer>
            <Icon icon="User" source="phosphor" size="xl" />
            {placeholder}
        </PlaceholderContainer>
    );
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
            icon: mapRoleToPhosphorIcon(role.name),
        }));

        // Sort roles in desired order: Reader, Edit, Admin
        const roleOrder = ['Reader', 'Edit', 'Admin'];
        const sortedRoleOptions = roleOptions.sort((a, b) => {
            const indexA = roleOrder.indexOf(a.label);
            const indexB = roleOrder.indexOf(b.label);

            // If both roles are in the order array, sort by their position
            if (indexA !== -1 && indexB !== -1) {
                return indexA - indexB;
            }
            // If only one is in the order array, prioritize it
            if (indexA !== -1) return -1;
            if (indexB !== -1) return 1;
            // If neither is in the order array, maintain original order
            return 0;
        });

        // Add "No Role" option at the end
        return [...sortedRoleOptions, { value: '', label: placeholder, icon: mapRoleToPhosphorIcon('') }];
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
        <Tooltip title="Set user role" placement="top" open={isDropdownOpen ? false : undefined}>
            <span>
                <SimpleSelect
                    onUpdate={(values) => handleRoleSelect(values[0] || '')}
                    onOpenChange={setIsDropdownOpen}
                    options={roleSelectOptions}
                    placeholder={placeholderWithIcon}
                    values={selectedRole?.urn ? [selectedRole.urn] : []}
                    size={size}
                    width={width}
                    isDisabled={disabled}
                    showClear={false}
                />
            </span>
        </Tooltip>
    );
}

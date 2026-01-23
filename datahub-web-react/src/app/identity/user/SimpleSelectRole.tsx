import { LoadingOutlined } from '@ant-design/icons';
import { SimpleSelect, Text, Tooltip } from '@components';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { SelectOption } from '@components/components/Select/types';

import { mapRoleToPhosphorIcon } from '@app/identity/user/PhosphorRoleUtils';
import { useRoleSelector } from '@app/identity/user/useRoleSelector';

import { DataHubRole } from '@types';

const LoadMoreContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 8px;
`;

const OptionContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const LOAD_MORE_VALUE = '__load_more_sentinel__';

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
    const { roles, loading, hasMore, observerRef, setSearchQuery } = useRoleSelector();

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
        const options = [...sortedRoleOptions, { value: '', label: placeholder, icon: mapRoleToPhosphorIcon('') }];

        // Add sentinel option for infinite scroll trigger
        if (hasMore) {
            options.push({ value: LOAD_MORE_VALUE, label: 'Loading more...', icon: <LoadingOutlined /> });
        }

        return options;
    }, [roles, placeholder, hasMore]);

    const handleRoleSelect = (roleUrn: string) => {
        // Ignore clicks on the sentinel option
        if (roleUrn === LOAD_MORE_VALUE) return;

        if (roleUrn === '') {
            onRoleSelect(undefined);
        } else {
            const selectedRoleFromList = roles.find((role) => role.urn === roleUrn);
            onRoleSelect(selectedRoleFromList);
        }
    };

    const renderOptionText = (option: SelectOption) => {
        // Render the sentinel option with the observer div for infinite scroll
        if (option.value === LOAD_MORE_VALUE) {
            return (
                <LoadMoreContainer ref={observerRef}>
                    <LoadingOutlined />
                    <Text color="gray" size="sm" style={{ marginLeft: 8 }}>
                        Loading more roles...
                    </Text>
                </LoadMoreContainer>
            );
        }

        // Default rendering for role options
        return (
            <Tooltip title={option.value || 'No URN'} placement="right">
                <OptionContainer>
                    {option.icon}
                    <Text weight="semiBold" size="md" color="gray">
                        {option.label}
                    </Text>
                </OptionContainer>
            </Tooltip>
        );
    };

    return (
        <Tooltip title="Set user role" placement="top">
            <span>
                <SimpleSelect
                    onUpdate={(values) => handleRoleSelect(values[0] || '')}
                    options={roleSelectOptions}
                    placeholder={placeholder}
                    values={selectedRole?.urn ? [selectedRole.urn] : []}
                    size={size}
                    width={width}
                    isDisabled={disabled}
                    showClear={false}
                    showSearch
                    onSearchChange={setSearchQuery}
                    filterResultsByQuery={false}
                    isLoading={loading && roles.length === 0}
                    renderCustomOptionText={renderOptionText}
                />
            </span>
        </Tooltip>
    );
}

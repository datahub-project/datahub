import { TeamOutlined } from '@ant-design/icons';
import { useApolloClient } from '@apollo/client';
import { Select, Tag } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import AssignRoletoGroupConfirmation from '@app/identity/group/AssignRoletoGroupConfirmation';
import { mapRoleIcon } from '@app/identity/user/UserUtils';
import { clearRoleListCache } from '@app/permissions/roles/cacheUtils';

import { CorpGroup, DataHubRole } from '@types';

const NO_ROLE_TEXT = 'No Roles';

type Props = {
    group: CorpGroup;
    groupRoleUrns: string[];
    selectRoleOptions: Array<DataHubRole>;
    refetch?: () => void;
};

const RoleSelect = styled(Select)<{ color?: string }>`
    min-width: 150px;
    ${(props) => (props.color ? ` color: ${props.color};` : '')}
`;

const RoleIcon = styled.span`
    margin-right: 6px;
    font-size: 12px;
`;

/**
 * Custom tag renderer for selected roles
 */
const RoleTag = ({ label, closable, onClose, value, rolesMap }: any) => {
    const role = rolesMap.get(value);
    const onPreventMouseDown = (event: React.MouseEvent) => {
        event.preventDefault();
        event.stopPropagation();
    };

    return (
        <Tag
            onMouseDown={onPreventMouseDown}
            closable={closable}
            onClose={onClose}
            style={{
                marginRight: 3,
                display: 'flex',
                alignItems: 'center',
                padding: '2px 6px',
                border: 'none',
            }}
        >
            <RoleIcon>{mapRoleIcon(role?.name || '')}</RoleIcon>
            {label}
        </Tag>
    );
};

export default function MultiSelectRoleGroup({ group, groupRoleUrns, selectRoleOptions, refetch }: Props) {
    const client = useApolloClient();
    const rolesMap: Map<string, DataHubRole> = new Map();
    selectRoleOptions.forEach((role) => {
        rolesMap.set(role.urn, role);
    });

    const selectOptions = selectRoleOptions.map((role) => {
        return (
            <Select.Option key={role.urn} value={role.urn}>
                {role.name}
            </Select.Option>
        );
    });

    const [currentRoleUrns, setCurrentRoleUrns] = useState<string[]>(groupRoleUrns);
    const [isViewingAssignRole, setIsViewingAssignRole] = useState(false);
    const [roleChangeInfo, setRoleChangeInfo] = useState<{
        rolesToAdd: DataHubRole[];
        rolesToRemove: DataHubRole[];
    }>({ rolesToAdd: [], rolesToRemove: [] });

    useEffect(() => {
        setCurrentRoleUrns(groupRoleUrns);
    }, [groupRoleUrns]);

    /** Handles role selection changes and shows confirmation if needed */
    const onSelectRoles = (selectedUrns: unknown) => {
        const urns = selectedUrns as string[];
        const currentSet = new Set(groupRoleUrns);
        const selectedSet = new Set(urns);

        // Calculate role changes
        const rolesToAdd = urns
            .filter((urn) => !currentSet.has(urn))
            .map((urn) => rolesMap.get(urn))
            .filter(Boolean) as DataHubRole[];

        const rolesToRemove = groupRoleUrns
            .filter((urn) => !selectedSet.has(urn))
            .map((urn) => rolesMap.get(urn))
            .filter(Boolean) as DataHubRole[];

        setCurrentRoleUrns(urns);
        setRoleChangeInfo({ rolesToAdd, rolesToRemove });

        // Show confirmation only if there are changes
        const hasChanges = rolesToAdd.length > 0 || rolesToRemove.length > 0;
        if (hasChanges) {
            setIsViewingAssignRole(true);
        }
    };

    const onCancel = () => {
        setCurrentRoleUrns(groupRoleUrns);
        setIsViewingAssignRole(false);
        setRoleChangeInfo({ rolesToAdd: [], rolesToRemove: [] });
    };

    const onConfirm = () => {
        setIsViewingAssignRole(false);
        setRoleChangeInfo({ rolesToAdd: [], rolesToRemove: [] });
        // Immediate refetch to update the UI
        refetch?.();
        clearRoleListCache(client); // Update roles.
    };

    // Wait for available roles to load
    if (!selectRoleOptions.length) return null;

    return (
        <>
            <RoleSelect
                mode="multiple"
                placeholder={
                    <>
                        <TeamOutlined style={{ marginRight: 6, fontSize: 12 }} />
                        {NO_ROLE_TEXT}
                    </>
                }
                value={currentRoleUrns}
                onChange={onSelectRoles}
                color={currentRoleUrns.length === 0 ? ANTD_GRAY[6] : undefined}
                tagRender={(props) => <RoleTag {...props} rolesMap={rolesMap} />}
                showSearch
                filterOption={(input, option) =>
                    (option?.children as any)?.props?.children?.[1]?.toLowerCase().includes(input.toLowerCase()) ||
                    false
                }
            >
                {selectOptions}
            </RoleSelect>
            <AssignRoletoGroupConfirmation
                open={isViewingAssignRole}
                roleToAssign={roleChangeInfo.rolesToAdd[0] || roleChangeInfo.rolesToRemove[0]} // Use first role for confirmation dialog
                groupUrn={group.urn}
                groupName={group?.info?.displayName as string}
                onClose={onCancel}
                onConfirm={onConfirm}
                // Pass additional info for multi-role changes
                multiRoleChanges={{
                    rolesToAdd: roleChangeInfo.rolesToAdd,
                    rolesToRemove: roleChangeInfo.rolesToRemove,
                    currentRoles: selectRoleOptions.filter((role) => groupRoleUrns.includes(role.urn)),
                }}
            />
        </>
    );
}

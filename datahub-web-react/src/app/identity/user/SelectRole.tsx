import React, { useState } from 'react';
import { UserOutlined } from '@ant-design/icons';
import { Select } from 'antd';
import styled from 'styled-components';
import { CorpUser, DataHubRole } from '../../../types.generated';
import AssignRoleConfirmation from './AssignRoleConfirmation';
import { mapRoleIcon } from './UserUtils';

const NO_ROLE_TEXT = 'No Role';

type Props = {
    user: CorpUser;
    userRoleUrn: string;
    selectRoleOptions: Array<DataHubRole>;
    refetch?: () => void;
};

const RoleSelect = styled(Select)`
    min-width: 105px;
`;

const RoleIcon = styled.span`
    margin-right: 6px;
    font-size: 12px;
`;

export default function SelectRole({ user, userRoleUrn, selectRoleOptions, refetch }: Props) {
    const rolesMap: Map<string, DataHubRole> = new Map();
    selectRoleOptions.forEach((role) => {
        rolesMap.set(role.urn, role);
    });
    const allSelectRoleOptions = [{ urn: '', name: NO_ROLE_TEXT }, ...selectRoleOptions];
    const selectOptions = () =>
        allSelectRoleOptions.map((role) => {
            return (
                <Select.Option value={role.urn}>
                    <RoleIcon>{mapRoleIcon(role.name)}</RoleIcon>
                    {role.name}
                </Select.Option>
            );
        });

    const initialRole = rolesMap.get(userRoleUrn) as DataHubRole;
    const [currentRole, setCurrentRole] = useState<DataHubRole | undefined>(initialRole);
    const [isViewingAssignRole, setIsViewingAssignRole] = useState(false);

    const onSelectRole = (roleUrn: string) => {
        const roleFromMap: DataHubRole = rolesMap.get(roleUrn) as DataHubRole;
        setCurrentRole(roleFromMap);
        setIsViewingAssignRole(true);
    };

    const onCancel = () => {
        setCurrentRole(initialRole);
        setIsViewingAssignRole(false);
    };

    const onConfirm = () => {
        setIsViewingAssignRole(false);
        setTimeout(function () {
            refetch?.();
        }, 3000);
    };

    return (
        <>
            <RoleSelect
                placeholder={
                    <>
                        <UserOutlined style={{ marginRight: 6, fontSize: 12 }} />
                        {NO_ROLE_TEXT}
                    </>
                }
                value={currentRole?.urn}
                onChange={(e) => onSelectRole(e as string)}
            >
                {selectOptions()}
            </RoleSelect>
            <AssignRoleConfirmation
                visible={isViewingAssignRole}
                roleToAssign={currentRole}
                userUrn={user.urn}
                username={user.username}
                onClose={onCancel}
                onConfirm={onConfirm}
            />
        </>
    );
}

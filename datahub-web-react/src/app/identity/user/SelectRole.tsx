import React, { useState } from 'react';
import { UserOutlined } from '@ant-design/icons';
import { Select } from 'antd';
import styled from 'styled-components';
import { CorpUser, DataHubRole } from '../../../types.generated';
import AssignRoleConfirmation from './AssignRoleConfirmation';
import { mapRoleIcon } from './UserUtils';

const NO_ROLE_TEXT = 'No Role';
const NO_ROLE_URN = 'urn:li:dataHubRole:NoRole';

type Props = {
    user: CorpUser;
    userRoleUrn?: string;
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
    const allSelectRoleOptions = [{ urn: NO_ROLE_URN, name: NO_ROLE_TEXT }, ...selectRoleOptions];
    const selectOptions = allSelectRoleOptions.map((role) => {
        return (
            <Select.Option key={role.urn} value={role.urn}>
                <RoleIcon>{mapRoleIcon(role.name)}</RoleIcon>
                {role.name}
            </Select.Option>
        );
    });

    const [currentRole, setCurrentRole] = useState<string | undefined>(userRoleUrn || undefined);
    const [isViewingAssignRole, setIsViewingAssignRole] = useState(false);

    const onSelectRole = (roleUrn: string) => {
        setCurrentRole(roleUrn === NO_ROLE_URN ? undefined : roleUrn);
        setIsViewingAssignRole(true);
    };

    const onCancel = () => {
        setCurrentRole(userRoleUrn || undefined);
        setIsViewingAssignRole(false);
    };

    const onConfirm = () => {
        setIsViewingAssignRole(false);
        setTimeout(function () {
            refetch?.();
        }, 3000);
    };

    // wait for available roles to load
    if (!selectRoleOptions.length) return null;

    return (
        <>
            <RoleSelect
                placeholder={
                    <>
                        <UserOutlined style={{ marginRight: 6, fontSize: 12 }} />
                        {NO_ROLE_TEXT}
                    </>
                }
                value={currentRole}
                onChange={(e) => onSelectRole(e as string)}
            >
                {selectOptions}
            </RoleSelect>
            <AssignRoleConfirmation
                visible={isViewingAssignRole}
                roleToAssign={rolesMap.get(currentRole || '')}
                userUrn={user.urn}
                username={user.username}
                onClose={onCancel}
                onConfirm={onConfirm}
            />
        </>
    );
}

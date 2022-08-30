import React, { useState } from 'react';
import { UserOutlined } from '@ant-design/icons';
import { Select } from 'antd';
import styled from 'styled-components';
import { CorpUser, DataHubRole } from '../../../types.generated';
import AssignRoleConfirmation from './AssignRoleConfirmation';
import { mapRoleIcon } from './UserUtils';

type Props = {
    user: CorpUser;
    userRoleUrn: string;
    roles: Array<DataHubRole>;
    refetch?: () => void;
};

const RoleSelect = styled(Select)`
    min-width: 100px;
`;

const RoleIcon = styled.span`
    margin-right: 6px;
    font-size: 12px;
`;

export default function SelectRole({ user, userRoleUrn, roles, refetch }: Props) {
    const [isViewingAssignRole, setIsViewingAssignRole] = useState(false);
    const [roleToAssign, setRoleToAssign] = useState<DataHubRole>();

    const rolesMap: Map<string, DataHubRole> = new Map();
    roles.forEach((role) => {
        rolesMap.set(role.urn, role);
    });

    const selectOptions = () =>
        roles.map((role) => {
            return (
                <Select.Option value={role.urn}>
                    <RoleIcon>{mapRoleIcon(role.name)}</RoleIcon>
                    {role.name}
                </Select.Option>
            );
        });

    const onSelectRole = (roleUrn: string) => {
        const roleFromMap: DataHubRole = rolesMap.get(roleUrn) as DataHubRole;
        setRoleToAssign(roleFromMap);
        setIsViewingAssignRole(true);
    };

    return (
        <>
            <RoleSelect
                placeholder={
                    <RoleIcon>
                        <UserOutlined />
                        No Role
                    </RoleIcon>
                }
                value={userRoleUrn || undefined}
                onChange={(e) => onSelectRole(e as string)}
            >
                {selectOptions()}
            </RoleSelect>
            <AssignRoleConfirmation
                visible={isViewingAssignRole}
                roleToAssign={roleToAssign}
                userUrn={user.urn}
                username={user.username}
                onClose={() => setIsViewingAssignRole(false)}
                onConfirm={() => {
                    setIsViewingAssignRole(false);
                    setTimeout(function () {
                        refetch?.();
                    }, 3000);
                }}
            />
        </>
    );
}

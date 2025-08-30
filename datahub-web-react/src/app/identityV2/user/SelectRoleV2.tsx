import { useApolloClient } from '@apollo/client';
import { Select } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import AssignRoleConfirmation from '@app/identity/user/AssignRoleConfirmation';
import { clearRoleListCache } from '@app/permissions/roles/cacheUtils';
import { mapRoleIconV2 } from '@app/identityV2/user/UserUtilsV2';
import { User as UserIcon } from '@phosphor-icons/react';

import { CorpUser, DataHubRole } from '@types';

const NO_ROLE_TEXT = 'No Role';
const NO_ROLE_URN = 'urn:li:dataHubRole:NoRole';

type Props = {
    user: CorpUser;
    userRoleUrn: string;
    selectRoleOptions: Array<DataHubRole>;
    refetch?: () => void;
};

const RoleSelect = styled(Select)<{ color?: string }>`
    min-width: 105px;
    ${(props) => (props.color ? ` color: ${props.color};` : '')}
`;

const RoleIcon = styled.span`
    margin-right: 6px;
    font-size: 12px;
`;

export default function SelectRoleV2({ user, userRoleUrn, selectRoleOptions, refetch }: Props) {
    const client = useApolloClient();
    const rolesMap: Map<string, DataHubRole> = new Map();
    selectRoleOptions.forEach((role) => {
        rolesMap.set(role.urn, role);
    });
    const allSelectRoleOptions = [{ urn: NO_ROLE_URN, name: NO_ROLE_TEXT }, ...selectRoleOptions];
    const selectOptions = allSelectRoleOptions.map((role) => {
        return (
            <Select.Option key={role.urn} value={role.urn}>
                <RoleIcon>{mapRoleIconV2(role.name)}</RoleIcon>
                {role.name}
            </Select.Option>
        );
    });

    const defaultRoleUrn = userRoleUrn || NO_ROLE_URN;
    const [currentRoleUrn, setCurrentRoleUrn] = useState<string>(defaultRoleUrn);
    const [isViewingAssignRole, setIsViewingAssignRole] = useState(false);

    useEffect(() => {
        setCurrentRoleUrn(defaultRoleUrn);
    }, [defaultRoleUrn]);

    const onSelectRole = (roleUrn: string) => {
        setCurrentRoleUrn(roleUrn);
        setIsViewingAssignRole(true);
    };

    const onCancel = () => {
        setCurrentRoleUrn(defaultRoleUrn);
        setIsViewingAssignRole(false);
    };

    const onConfirm = () => {
        setIsViewingAssignRole(false);
        setTimeout(() => {
            refetch?.();
            clearRoleListCache(client); // Update roles.
        }, 3000);
    };

    if (!selectRoleOptions.length) return null;

    return (
        <>
            <RoleSelect
                placeholder={
                    <>
                        <UserIcon style={{ marginRight: 6, fontSize: 12 }} />
                        {NO_ROLE_TEXT}
                    </>
                }
                value={currentRoleUrn}
                onChange={(e) => onSelectRole(e as string)}
                color={currentRoleUrn === NO_ROLE_URN ? ANTD_GRAY[6] : undefined}
            >
                {selectOptions}
            </RoleSelect>
            <AssignRoleConfirmation
                open={isViewingAssignRole}
                roleToAssign={rolesMap.get(currentRoleUrn)}
                userUrn={user.urn}
                username={user.username}
                onClose={onCancel}
                onConfirm={onConfirm}
            />
        </>
    );
}


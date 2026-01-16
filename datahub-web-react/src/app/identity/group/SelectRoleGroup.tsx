import { UserOutlined } from '@ant-design/icons';
import { useApolloClient } from '@apollo/client';
import { Select, Spin } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import AssignRoletoGroupConfirmation from '@app/identity/group/AssignRoletoGroupConfirmation';
import { mapRoleIcon } from '@app/identity/user/UserUtils';
import { clearRoleListCache } from '@app/permissions/roles/cacheUtils';

import { CorpGroup, DataHubRole } from '@types';

const NO_ROLE_TEXT = 'No Role';
const NO_ROLE_URN = 'urn:li:dataHubRole:NoRole';

type Props = {
    group: CorpGroup;
    groupRoleUrn: string;
    selectRoleOptions: Array<DataHubRole>;
    rolesLoading: boolean;
    rolesHasMore: boolean;
    rolesObserverRef: (node: HTMLDivElement | null) => void;
    rolesSearchQuery: string;
    setRolesSearchQuery: (query: string) => void;
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

const LoadMoreSentinel = styled.div`
    display: flex;
    justify-content: center;
    padding: 8px;
`;

const DropdownContainer = styled.div`
    max-height: 300px;
    overflow-y: auto;
`;

export default function SelectRoleGroup({
    group,
    groupRoleUrn,
    selectRoleOptions,
    rolesLoading,
    rolesHasMore,
    rolesObserverRef,
    rolesSearchQuery,
    setRolesSearchQuery,
    refetch,
}: Props) {
    const client = useApolloClient();
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

    const defaultRoleUrn = groupRoleUrn || NO_ROLE_URN;
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

    const handleDropdownVisibleChange = (open: boolean) => {
        if (!open) {
            setRolesSearchQuery('');
        }
    };

    const dropdownRender = (menu: React.ReactNode) => (
        <DropdownContainer>
            {menu}
            {rolesHasMore && (
                <LoadMoreSentinel ref={rolesObserverRef}>
                    <Spin size="small" />
                </LoadMoreSentinel>
            )}
        </DropdownContainer>
    );

    // wait for available roles to load on initial render
    if (!selectRoleOptions.length && !rolesSearchQuery && rolesLoading) return null;

    return (
        <>
            <RoleSelect
                placeholder={
                    <>
                        <UserOutlined style={{ marginRight: 6, fontSize: 12 }} />
                        {NO_ROLE_TEXT}
                    </>
                }
                value={currentRoleUrn}
                onChange={(e) => onSelectRole(e as string)}
                color={currentRoleUrn === NO_ROLE_URN ? ANTD_GRAY[6] : undefined}
                showSearch
                filterOption={false}
                searchValue={rolesSearchQuery}
                onSearch={setRolesSearchQuery}
                onDropdownVisibleChange={handleDropdownVisibleChange}
                dropdownRender={dropdownRender}
                loading={rolesLoading}
                notFoundContent={rolesLoading ? <Spin size="small" /> : 'No roles found'}
            >
                {selectOptions}
            </RoleSelect>
            <AssignRoletoGroupConfirmation
                open={isViewingAssignRole}
                roleToAssign={rolesMap.get(currentRoleUrn)}
                groupUrn={group.urn}
                groupName={group?.info?.displayName as string}
                onClose={onCancel}
                onConfirm={onConfirm}
            />
        </>
    );
}

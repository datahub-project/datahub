import { Avatar, Pagination, Pill, SearchBar, Table, Text, Tooltip } from '@components';
import * as QueryString from 'query-string';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import AvatarStackWithHover from '@components/components/AvatarStack/AvatarStackWithHover';
import { AvatarItemProps, AvatarType } from '@components/components/AvatarStack/types';

import DataAccessRoleDetailsModal from '@app/permissions/dataAccessRoles/DataAccessRoleDetailsModal';
import {
    DataAccessRole,
    DataAccessRoleGroup,
    DataAccessRoleUser,
    getDataAccessRoleGroups,
    getDataAccessRoleUsers,
} from '@app/permissions/dataAccessRoles/dataAccessRoles.utils';
import { DEBOUNCE_SEARCH_MS } from '@app/shared/constants';
import { ToastType, showToastMessage } from '@app/sharedV2/toastMessageUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useListDataAccessRolesQuery } from '@graphql/dataAccessRole.generated';
import { EntityType } from '@types';

const TableScrollContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    overflow: auto;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const RoleName = styled.span`
    font-weight: 700;
`;

const PageContainer = styled.div`
    width: 100%;
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding-top: 16px;
    overflow: hidden;
`;

const PillsContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
`;

const EmptyContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 40px;
`;

const DEFAULT_PAGE_SIZE = 10;

function toAvatar(
    actor: DataAccessRoleUser | DataAccessRoleGroup,
    entityRegistry: ReturnType<typeof useEntityRegistry>,
): AvatarItemProps {
    const isGroup = actor.urn?.startsWith('urn:li:corpGroup');
    return {
        name: entityRegistry.getDisplayName(isGroup ? EntityType.CorpGroup : EntityType.CorpUser, actor),
        imageUrl: actor.editableProperties?.pictureLink || undefined,
        urn: actor.urn,
        type: isGroup ? AvatarType.group : AvatarType.user,
    };
}

export const ManageDataAccessRoles = () => {
    const { t } = useTranslation('settings.permissions');
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [focusRole, setFocusRole] = useState<DataAccessRole>();
    const [showViewRoleModal, setShowViewRoleModal] = useState(false);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const [page, setPage] = useState(1);
    const pageSize = DEFAULT_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const { loading, error, data } = useListDataAccessRolesQuery({
        variables: {
            input: {
                type: EntityType.Role,
                query: query || '*',
                start,
                count: pageSize,
            },
        },
        fetchPolicy: (query?.length || 0) > 0 ? 'no-cache' : 'cache-first',
    });

    const totalRoles = data?.search?.total || 0;
    const roles = useMemo(
        () =>
            (data?.search?.searchResults || [])
                .map((result) => result.entity)
                .filter((entity): entity is DataAccessRole => entity.__typename === 'Role'),
        [data],
    );

    const onViewRole = (role: DataAccessRole) => {
        setFocusRole(role);
        setShowViewRoleModal(true);
    };

    const tableColumns = [
        {
            title: t('column.name'),
            key: 'name',
            width: '20%',
            render: (record: { name: string }) => <RoleName>{record.name}</RoleName>,
        },
        {
            title: t('column.description'),
            key: 'description',
            width: '30%',
            render: (record: { description: string }) => record.description,
        },
        {
            title: t('column.type'),
            key: 'accessType',
            width: '15%',
            render: (record: { accessType: string }) =>
                record.accessType ? (
                    <PillsContainer>
                        <Pill label={record.accessType} variant="outline" color="gray" size="sm" clickable={false} />
                    </PillsContainer>
                ) : null,
        },
        {
            title: t('usersLabel'),
            key: 'users',
            width: '35%',
            render: (record: { actors: Array<DataAccessRoleUser | DataAccessRoleGroup> }) => {
                const actors: AvatarItemProps[] = record.actors.map((actor) => toAvatar(actor, entityRegistry));
                if (!actors.length) {
                    return null;
                }
                if (actors.length === 1) {
                    return (
                        <Tooltip title={actors[0].name}>
                            <Avatar
                                name={actors[0].name}
                                imageUrl={actors[0].imageUrl}
                                type={actors[0].type}
                                size="sm"
                                showInPill
                            />
                        </Tooltip>
                    );
                }
                return (
                    <AvatarStackWithHover
                        avatars={actors}
                        maxToShow={5}
                        size="sm"
                        showRemainingNumber
                        entityRegistry={entityRegistry as any}
                        title={t('usersLabel')}
                    />
                );
            },
        },
    ];

    const tableData = roles.map((role) => ({
        role,
        urn: role.urn,
        name: role.properties?.name || '',
        description: role.properties?.description || '',
        accessType: role.properties?.type || '',
        actors: [...getDataAccessRoleUsers(role), ...getDataAccessRoleGroups(role)],
    }));

    return (
        <PageContainer>
            {error && showToastMessage(ToastType.ERROR, t('dataAccessRoles.loadError'), 3)}
            <SearchBar
                placeholder={t('dataAccessRoles.searchPlaceholder')}
                value={query || ''}
                onChange={(value) => {
                    const nextQuery = value || undefined;
                    if (nextQuery === query) {
                        return;
                    }
                    setPage(1);
                    setQuery(nextQuery);
                }}
                debounceDelay={DEBOUNCE_SEARCH_MS}
                width="300px"
                allowClear
            />
            <TableScrollContainer>
                {!loading && tableData.length === 0 ? (
                    <EmptyContainer>
                        <Text size="md" color="gray">
                            {t('dataAccessRoles.empty')}
                        </Text>
                    </EmptyContainer>
                ) : (
                    <Table
                        columns={tableColumns}
                        data={tableData}
                        rowKey="urn"
                        isScrollable
                        isLoading={loading}
                        style={{ tableLayout: 'fixed' }}
                        onRowClick={(record) => onViewRole(record.role)}
                    />
                )}
            </TableScrollContainer>
            <PaginationContainer>
                <Pagination currentPage={page} itemsPerPage={pageSize} total={totalRoles} onPageChange={setPage} />
            </PaginationContainer>
            <DataAccessRoleDetailsModal
                role={focusRole}
                open={showViewRoleModal}
                onClose={() => setShowViewRoleModal(false)}
            />
        </PageContainer>
    );
};

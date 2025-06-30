import { UsergroupAddOutlined } from '@ant-design/icons';
import { Button, Empty, List, Pagination } from 'antd';
import * as QueryString from 'query-string';
import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import CreateGroupModal from '@app/identity/group/CreateGroupModal';
import GroupListItem from '@app/identity/group/GroupListItem';
import {
    DEFAULT_GROUP_LIST_PAGE_SIZE,
    addGroupToListGroupsCache,
    removeGroupFromListGroupsCache,
} from '@app/identity/group/cacheUtils';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import { GROUPS_CREATE_GROUP_ID, GROUPS_INTRO_ID } from '@app/onboarding/config/GroupsOnboardingConfig';
import { SearchBar } from '@app/search/SearchBar';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useListGroupsQuery } from '@graphql/group.generated';
import { useListRolesQuery } from '@graphql/role.generated';
import { CorpGroup, DataHubRole } from '@types';

const GroupContainer = styled.div`
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

const GroupStyledList = styled(List)`
    display: flex;
    flex-direction: column;
    overflow: auto;
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
    }
`;

const GroupPaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

export const GroupList = () => {
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<undefined | string>(undefined);
    useEffect(() => setQuery(paramsQuery), [paramsQuery]);

    const [page, setPage] = useState(1);
    const [isCreatingGroup, setIsCreatingGroup] = useState(false);

    // Policy list paging.
    const pageSize = DEFAULT_GROUP_LIST_PAGE_SIZE;
    const start = (page - 1) * pageSize;

    const {
        loading,
        error,
        data,
        refetch: groupRefetch,
        client,
    } = useListGroupsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query: (query?.length && query) || undefined,
            },
        },
        fetchPolicy: (query?.length || 0) > 0 ? 'no-cache' : 'cache-first',
    });

    const totalGroups = data?.listGroups?.total || 0;
    const groups = data?.listGroups?.groups || [];

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    const handleDelete = (urn: string) => {
        removeGroupFromListGroupsCache(urn, client, page, pageSize);
    };

    const { data: rolesData } = useListRolesQuery({
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                start: 0,
                count: 10,
            },
        },
    });

    const selectRoleOptions = rolesData?.listRoles?.roles?.map((role) => role as DataHubRole) || [];

    return (
        <>
            <OnboardingTour stepIds={[GROUPS_INTRO_ID, GROUPS_CREATE_GROUP_ID]} />
            {!data && loading && <Message type="loading" content="Loading groups..." />}
            {error && <Message type="error" content="Failed to load groups! An unexpected error occurred." />}
            <GroupContainer>
                <TabToolbar>
                    <Button id={GROUPS_CREATE_GROUP_ID} type="text" onClick={() => setIsCreatingGroup(true)}>
                        <UsergroupAddOutlined /> Create group
                    </Button>
                    <SearchBar
                        initialQuery={query || ''}
                        placeholderText="Search groups..."
                        suggestions={[]}
                        style={{
                            maxWidth: 220,
                            padding: 0,
                        }}
                        inputStyle={{
                            height: 32,
                            fontSize: 12,
                        }}
                        onSearch={() => null}
                        onQueryChange={(q) => {
                            setPage(1);
                            setQuery(q);
                        }}
                        entityRegistry={entityRegistry}
                        hideRecommendations
                    />
                </TabToolbar>
                <GroupStyledList
                    bordered
                    locale={{
                        emptyText: <Empty description="No Groups!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    dataSource={groups}
                    renderItem={(item: any) => (
                        <GroupListItem
                            onDelete={() => handleDelete(item.urn)}
                            group={item as CorpGroup}
                            selectRoleOptions={selectRoleOptions}
                            refetch={groupRefetch}
                        />
                    )}
                />
                <GroupPaginationContainer>
                    <Pagination
                        style={{ margin: 40 }}
                        current={page}
                        pageSize={pageSize}
                        total={totalGroups}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </GroupPaginationContainer>
                {isCreatingGroup && (
                    <CreateGroupModal
                        onClose={() => setIsCreatingGroup(false)}
                        onCreate={(group: CorpGroup) => {
                            addGroupToListGroupsCache(group, client);
                            setTimeout(() => groupRefetch(), 3000);
                        }}
                    />
                )}
            </GroupContainer>
        </>
    );
};
